from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    load_sql = '''
        INSERT INTO {}
        {};
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 table = '',
                 sql_insert = '',
                 delete = True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_insert = sql_insert
        self.delete = delete

    def execute(self, context):
        self.log.info('Connecting to redshift')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.delete:
            self.log.info('Deleting {self.table} data')
            redshift_hook.run('DELETE FROM {self.table}')
        
        self.log.info('Loading data to {self.table}')
        formatted_sql = LoadDimensionOperator.load_sql.format(
            self.table,
            self.sql_insert
        )

        redshift_hook.run(formatted_sql)
