from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = '''
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}';
    '''

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 aws_credentials_id = '',
                 table = '',
                 s3_path = '',
                 json_path = '',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_path = s3_path
        self.execution_time = kwargs.get('execution_time')

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)

        self.log.info('Getting aws credentials')
        credentials = aws_hook.get_credentials()

        self.log.info('Connecting to redshift')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info('Copying data from {self.s3_path} to {self.table} redshift table')
        if self.execution_time:
            s3_path_execution_time = '{self.s3_path}/{self.execution_time.strftime("%Y")}/{self.execution_time.strftime("%m")}'
    
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path_execution_time,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
        
        else:
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                self.s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.json_path
            )
        
        redshift_hook.run(formatted_sql)