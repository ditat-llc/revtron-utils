
import inspect
from datetime import datetime, timedelta
from typing import Any

import requests


class Salesforce:
	VERSION = 'v56.0'
	DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f+0000'
	DATE_FORMAT = '%Y-%m-%d'

	def __init__(self, session_id: str, client_id: str, client_secret: str):
		self.session_id = session_id
		self.client_id = client_id
		self.client_secret = client_secret
		self.refresh_token(session_id, client_id, client_secret)
		self.base_url = f'{self.instance_url}/services/data/{self.VERSION}/'

	def __repr__(self) -> str:
		params = {k: getattr(self, k) for k in inspect.signature(self.__init__)\
			.parameters.keys()}
		return f'<Salesforce {params}>'

	def __str__(self) -> str:
		return self.instance_url

	def request(
		self,
		method: str,
		url: str,
		headers: dict[str, Any] | bool | None = None,
		params: dict[str, Any] | None = None,
		data: dict[str, Any] | None = None,
		json: dict[str, Any] | None = None,
		**kwargs
	) -> dict[str, Any]:
		resp = requests.request(
			method=method,
			url=url,
			headers={
				"Content-Type": "application/json",
				'Authorization': f'Bearer {self.access_token}',
				**(headers if isinstance(headers, dict) else {})
			} if headers is not False else {},
			params=params,
			data=data,
			json=json,
			**kwargs
		)
		if resp.status_code not in [200, 201, 204]:
			raise Exception(f'Failed to make request: {resp.text}')
		if resp.status_code == 204:
			return {}
		return resp.json()

	def refresh_token(
		self,
		refresh_token: str,
		client_id: str,
		client_secret: str,
		environment: str = 'login'
	) -> None:
		try:
			resp = self.request(
				url=f'https://{environment}.salesforce.com/services/oauth2/token',
				method='POST',
				data={
					'grant_type': 'refresh_token',
					'refresh_token': refresh_token,
					'client_id': client_id,
					'client_secret': client_secret
				},
				headers=False
			)
			self.access_token = resp['access_token']
			self.instance_url = resp['instance_url']
		except:
			if environment == 'login':
				print('Wrong refresh token, trying test environment')
				self.refresh_token(
					refresh_token=refresh_token,
					client_id=client_id,
					client_secret=client_secret,
					environment='test'
				)
			else:
				raise Exception('Invalid refresh token')

	@property
	def sobjects(self) -> list[str]:
		resp = self.request('GET', url=f'{self.base_url}sobjects/')
		return [sobject['name'] for sobject in resp['sobjects']]

	def describe_sobject(self, sobject: str) -> dict[str, Any]:
		return self.request('GET', url=f'{self.base_url}sobjects/{sobject}/describe/')

	def get_sobject_columns(self, sobject: str) -> list[str]:
		return [field['name'] for field in self.describe_sobject(sobject)['fields']]

	@property
	def limits(self) -> dict[str, Any]:
		return self.request('GET', url=f'{self.base_url}limits/')['DailyApiRequests']

	def get(
		self,
		sobject: str,
		columns: list[str] | None = None,
		start_date: datetime | None = None,
		end_date: datetime | None = None,
		date_field: str = 'LastModifiedDate',
		limit: int | None = None,
		include_deleted: bool = True,
		exclude_attributes: bool = True,
		verbose: bool = False,
		**kwargs
	) -> Any:
		# Hard limit to allow FIELDS(ALL) to work
		batch_size = 100
		columns = columns or ["FIELDS(ALL)"]
		# Query Ids
		query = f"SELECT Id FROM {sobject}"
		# Dates
		if start_date:
			query += f' WHERE {date_field} >= {start_date.strftime(self.DATETIME_FORMAT)}'
		if end_date:
			query += f' AND {date_field} <= {end_date.strftime(self.DATETIME_FORMAT)}'
		# Other filters
		if kwargs:
			for key, value in kwargs.items():
				operator = 'IN' if isinstance(value, list) else '='
				if operator == 'IN':
					value = tuple(value) if len(value) > 1 else f"('{value[0]}')"
				else:
					value = f"'{value}'"
				query += f' AND {key} {operator} {value}'
		# Query check
		if ' WHERE ' not in query and ' AND ' in query:
			query = query.replace(' AND ', ' WHERE ', 1)
		# Limit
		if limit is not None:
			query += f' LIMIT {limit}'
		# Print		
		if verbose:
			print(query.replace('SELECT Id', f'SELECT {", ".join(columns)}'))
		# Query
		resp = self.request(
			'GET',
			url=f'{self.base_url}{"queryAll" if include_deleted else "query"}/',
			params={'q': query},
			headers={'Sforce-Query-Options': 'batchSize=200'}
		)
		total_size = resp['totalSize']
		print(f"Querying {total_size} records from {sobject}.")
		ids = [r['Id'] for r in resp['records']]
		while resp['done'] is False:
			resp = self.request(
				'GET',
				url=f"{self.instance_url}{resp['nextRecordsUrl']}",
				params={'q': query}
			)
			ids.extend([r['Id'] for r in resp['records']])
		# Batching Ids
		ids = [ids[i:i + batch_size] for i in range(0, len(ids), batch_size)]
		results = []
		# Building query
		query_data = f"SELECT {', '.join(columns)} FROM {sobject} WHERE Id IN "
		query_data = query_data + '{} LIMIT 200'
		# Querying data in batches
		for chunk in ids:
			q = query_data.format(
				tuple(chunk) if len(chunk) > 1 else f"('{chunk[0]}')"
			)
			url = f'{self.base_url}{"queryAll" if include_deleted else "query"}/'
			resp = self.request('GET', url=url, params={'q': q})
			results += resp['records']
			print(f"Retrieved {len(results)}/{total_size} records from {sobject}.")
		# Exclude attributes
		if exclude_attributes:
			results = [{k: v for k, v in record.items() if k != 'attributes'} \
				for record in results]
		return results

	def sobject_size(
		self,
		sobject: str,
		include_deleted: bool = True,
		date_window: int | None = None,
		start_date: datetime | None = None,
		end_date: datetime | None = None,
		date_field: str = 'LastModifiedDate',
		limit: int | None = None,
	) -> dict[str, int | str | None]:
		query = f'SELECT COUNT(Id), MIN({date_field}), MAX({date_field}) FROM {sobject}'
		if date_window is not None:
			start_date = datetime.now() - timedelta(days=date_window)
		if start_date is not None:
			query += f' WHERE {date_field} >= {start_date.strftime(self.DATETIME_FORMAT)}'
		if end_date is not None:
			operator = 'AND' if start_date is not None else 'WHERE'
			query += f' {operator} {date_field} <= {end_date.strftime(self.DATETIME_FORMAT)}'
		resp = self.request(
			'GET',
			url=f'{self.base_url}{"queryAll" if include_deleted else "query"}',
			params={'q': query}
		)['records'][0]
		rows = min(limit, resp['expr0']) if limit is not None else resp['expr0']
		min_date = resp['expr1']
		max_date = resp['expr2']
		columns = len(self.get_sobject_columns(sobject))
		return {
			'sobject': sobject,
			'rows': rows,
			'columns': columns,
			'min_date': min_date,
			'max_date': max_date,
			'limit': limit,
		}

	def update(self, sobject: str, Id: str, **kwargs) -> tuple[str, str]:
		self.request(
			'PATCH',
			url=f'{self.base_url}sobjects/{sobject}/{Id}',
			json=kwargs
		)
		return (sobject, Id)

	def insert(self, sobject: str, **kwargs) -> tuple[str, str]:
		resp = self.request(
			'POST',
			url=f'{self.base_url}sobjects/{sobject}',
			json=kwargs
		)
		return (sobject, resp['id'])


if __name__ == '__main__':
	pass
