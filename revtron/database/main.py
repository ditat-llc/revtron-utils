from typing import Any
import inspect

from pydantic import BaseModel
from sqlalchemy import (
	Table,
	Column,
	MetaData,
	create_engine,
	select,
	PrimaryKeyConstraint,
	UniqueConstraint,
	text,
	func,
	delete,
	update,
	bindparam,
	ForeignKey,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.schema import CreateTable
from sqlalchemy.inspection import inspect as sa_inspect



class Database:
	class ColumnModel(BaseModel):
		name: str
		type: Any
		default: Any | None = None
		server_default: Any | None = None
		autoincrement: bool | None = False
		foreign_key: ForeignKey | None = None

		class Config:
			arbitrary_types_allowed = True

	def __init__(self, db_url: str, schema: str = 'public'):
		self.db_url = db_url
		self.create_engine_from_url(db_url, schema)

	def create_engine_from_url(self, db_url: str, schema: str):
		try:
			self.engine = create_engine(db_url)
			self.schema = schema
			self.execute_raw('select 1 as is_alive;')
		except Exception as e:
			raise ValueError(f'Could not connect to database: {e}')

	def __str__(self) -> str:
		return f'Database({self.engine})'

	def __repr__(self) -> str:
		params = {k: getattr(self, k) for k in inspect.signature(self.__init__)\
			.parameters.keys()}
		return f'<Database {params}>'

	def get_table(self, table_name: str) -> Table:
		return Table(
			table_name,
			MetaData(),
			autoload=True,
			autoload_with=self.engine,
			schema=self.schema
		)
	
	def check_table_exists(self, table_name: str, schema: str | None = None):
		with self.engine.connect() as conn:
			return conn.dialect.has_table(conn, table_name, schema=schema or self.schema)
	
	def get_table_columns(self, table_name: str) -> list[str]:
		return [c.name for c in self.get_table(table_name).columns]

	def get_table_count(self, table_name: str) -> int | None:
		table = self.get_table(table_name)
		stmt = select([func.count()]).select_from(table)
		with self.engine.connect() as conn:
			count = conn.execute(stmt).scalar()
		return count

	def get_tables(self, schema: str | None = None) -> list[str]:
		return sa_inspect(self.engine).get_table_names(schema=schema or self.schema)

	def get_views(self, schema: str | None = None) -> list[str]:
		return sa_inspect(self.engine).get_view_names(schema=schema or self.schema)

	def upsert(
		self,
		table_name: str,
		data: dict | list[dict],
		chunk_size: int = 1_000,
		verbose: bool = False,
		overwrite_with_null: bool = False,
	) -> list[dict] | dict:
		table = self.get_table(table_name)
		index = [key.name for key in sa_inspect(table).primary_key]
		if not index:
			raise Exception(f'No primary key found for table {table_name}')
		chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]\
			if isinstance(data, list) else [[data]]
		results = []
		for number, chunk in enumerate(chunks):
			if verbose:
				print(f'Loading chunk {number + 1} of {len(chunks)}')
			stmt = insert(table).values(chunk).returning(*[table.c[c] for c in index])
			stmt = stmt.on_conflict_do_update(
				index_elements=index,
				set_={
					k: (stmt.excluded[k] if overwrite_with_null else \
					func.coalesce(stmt.excluded[k], table.c[k])) for k in chunk[0].keys()
				}
			)
			with self.engine.connect() as conn:
				res = conn.execute(stmt)
			results.extend([r._asdict() for r in res.fetchall()])
		return results[0] if isinstance(data, dict) else results

	def create_table(
		self,
		table_name: str,
		mappings: list[ColumnModel],
		primary_key: str | list[str] | None = None,
		unique_columns: list[str] | None = None,
		check_existing: bool = True,
		verbose: bool = False,
	) -> None:
		if check_existing and self.check_table_exists(table_name):
			existing_columns = self.get_table_columns(table_name)
			missing_columns = [
				(m.name, m.type) for m in mappings if m.name not in existing_columns
			]
			for column in missing_columns:
				self.add_column(
					table_name=table_name,
					column_name=column[0],
					column_type=column[1],
					verbose=verbose,
				)
		else:
			args = []
			for m in mappings:
				args.append(
					Column(
						m.name,
						m.type,
						m.foreign_key,
						default=m.default,
						server_default=m.server_default,
						autoincrement=m.autoincrement,
				   )
				)
			if primary_key:
				args.append(PrimaryKeyConstraint(*primary_key if \
					isinstance(primary_key, list) else [primary_key]))
			if unique_columns:
				for column in unique_columns:
					args.append(UniqueConstraint(column))
			table = Table(table_name, MetaData(), schema=self.schema, *args)
			table.create(self.engine, checkfirst=check_existing)
			if verbose:
				stmt = CreateTable(
					table,
					if_not_exists=check_existing
				).compile(self.engine)
				print(stmt)

	def add_column(
		self,
		table_name: str,
		column_name: str,
		column_type: Any,
		verbose: bool = False,
	):
		column_type = column_type() if isinstance(column_type, type) else column_type
		dialect_type = column_type.dialect_impl(self.engine.dialect)
		query = f'''
			ALTER TABLE {self.schema}."{table_name}"
			ADD COLUMN "{column_name}" {dialect_type}
			;
		'''
		if verbose:
			print(query)
		with self.engine.connect() as conn:
			conn.execute(query)

	def _where_clause(self, stmt: Any, table: Table, where: list[dict] | dict):
		where = [where] if isinstance(where, dict) else where
		for w in where:
			for k, v in w.items():
				if isinstance(v, dict):
					if v['operator'].lower() == 'in':
						stmt = stmt.where(table.c[k].in_(v['value']))
					elif v['operator'].lower() == 'not in':
						stmt = stmt.where(table.c[k].notin_(v['value']))
					elif v['operator'].lower() == 'like':
						stmt = stmt.where(table.c[k].like(v['value']))
					elif v['operator'].lower() == 'not like':
						stmt = stmt.where(table.c[k].notlike(v['value']))
					elif v['operator'].lower() == 'is null':
						stmt = stmt.where(table.c[k].is_(None))
					elif v['operator'].lower() == 'is not null':
						stmt = stmt.where(table.c[k].isnot(None))
					elif v['operator'].lower() == 'between':
						stmt = stmt.where(table.c[k].between(v['value'][0], v['value'][1]))
					elif v['operator'].lower() == 'not between':
						stmt = stmt.where(~table.c[k].between(v['value'][0], v['value'][1]))
					else:
						stmt = stmt.where(table.c[k].op(v['operator'])(v['value']))
				else:
					stmt = stmt.where(table.c[k] == v)
		return stmt

	def get(
		self,
		table_name: str,
		columns: list[str] | None = None,
		where: list[dict] | dict | None = None,
		limit: int | None = None,
		offset: int | None = None,
		sort_by: str | None = None,
		verbose: bool = False,
	) -> list[dict]:
		table = self.get_table(table_name)
		stmt = select([table.c[c] for c in columns] if columns else table.c)
		if where:
			stmt = self._where_clause(stmt, table, where)
		if offset:
			stmt = stmt.offset(offset)
		if sort_by:
			stmt = stmt.order_by(sort_by)
		if limit:
			stmt = stmt.limit(limit)
		results = []
		with self.engine.connect() as conn:
			for row in conn.execute(stmt).mappings():
				results.append(dict(row))
		if verbose:
			stmt = stmt.compile(self.engine)
			print(stmt)
		return results

	def update(
		self,
		table_name: str,
		data: dict | list[dict],
		on: str | list[str],
	) -> int:
		table = self.get_table(table_name)
		on = [on] if isinstance(on, str) else on
		data = [data] if isinstance(data, dict) else data
		stmt = update(table).returning(*[table.c[c] for c in on])
		for column in on:
			stmt = stmt.where(table.c[column] == bindparam(f"_{column}"))
		values = {k: bindparam(k) for k in data[0] if k not in on}
		stmt = stmt.values(values)
		for record in data:
			for column in on:
				record[f"_{column}"] = record.pop(column)
		with self.engine.connect() as conn:
			res = conn.execute(stmt, data)
		return res.rowcount

	def delete(
		self,
		table_name: str,
		where: dict[str, Any] | None = None,
		verbose: bool = False,
	) -> None:
		table = self.get_table(table_name)
		stmt = delete(table)
		if where:
			stmt = self._where_clause(stmt, table, where)
		if verbose:
			stmt = stmt.compile(self.engine)
			print(stmt)
		with self.engine.connect() as conn:
			conn.execute(stmt)

	def execute_raw(self, query: str) -> list[dict]:
		with self.engine.connect() as conn:
			response = conn.execute(text(query))
		result = []
		for row in response.mappings():
			result.append(dict(row))
		return result


if __name__ == '__main__':
	pass

