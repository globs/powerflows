import common.utils
import logging
import common.settings
import traceback
from common.orchestration.jobs.job_factory import FlowJobFactory


def main():
    try:
        common.settings.init_logging()
        logging.info('Staring commun.utils functions testing')
        test_yaml =  """
---
jobconfig:
  name: job test1
  kind: uploaddbfromcos
  group: dbjobs
  connections:
  - minio default
  - postgresql default
  operations:
  - operation:
      order: 1
      name: op test1
      connection_operation: import_data
      connection_target:
      - postgresql 
      connection_source:
      - minio default
      bucket_name: xx
      objects_names: 
      - obj1
      - obj2
      schema_name: xx
      table_name: xx
  - operation:
      order: 2
      name: op test2
      connection_operation: import_data
      connection_target:
      - postgresql 
      connection_source:
      - minio default
      bucket_name: xx
      objects_names: 
      - obj3
      - obj4
      schema_name: xx
      table_name: yy
        """
        jf = FlowJobFactory(job_yaml)
        jf.executeJob()
    except:
        logging.error(traceback.format_exc())

	


if __name__ == '__main__':
    main()
