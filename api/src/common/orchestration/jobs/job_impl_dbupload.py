from common.orchestration.jobs.job import FlowJob


class JobDBUpload(FlowJob):
    def __init__(self, config):
        super().__init__(config)

    def rolloutJob(self):
        config_dict = json.loads(self.config)
        run_res = []
        for jobconfig in config_dict:
            res_tmp = {
                "jobname" : jobconfig['jobname'],
            }
            logging.info(f"""
            job target db type {jobconfig['dbtype']}
            bucket_name {jobconfig['config']['bucket_name']}
            object_name {jobconfig['config']['object_name']}
            schema {jobconfig['config']['schema']}
            table {jobconfig['config']['table']}
            csv_colsep {jobconfig['config']['csv_colsep']}
            """)
            try:
                if jobconfig['mode'] == 'create':
                    self.methods['dbops'][jobconfig['dbtype']].dropTable(jobconfig['config']['schema'], jobconfig['config']['table'])
                    #self.methods['dbops'][jobconfig['dbtype']].executeQuery(f"DROP TABLE IF EXISTS {jobconfig['config']['schema']}.{jobconfig['config']['table']}")
                    self.methods['dbops'][jobconfig['dbtype']].executeQuery(jobconfig['config']['table_ddl'])
                else: 
                    if jobconfig['mode'] == 'replace':
                        self.methods['dbops'][jobconfig['dbtype']].executeQuery(f"DELETE FROM {jobconfig['config']['schema']}.{jobconfig['config']['table']}")
                for objname in jobconfig['config']['object_name'].split(','):
                    self.methods['csvloaders'][jobconfig['dbtype']](jobconfig['config']['bucket_name'],
                    objname,
                    jobconfig['config']['schema'],
                    jobconfig['config']['table'],
                    jobconfig['config']['csv_colsep'])
            except Exception as e:
                logging.error(f'something went wrong on job')
                res_tmp['error_message'] = f"something went wrong on job {e}"
                res_tmp['job status'] = f'ko'
                run_res.append(res_tmp)
                continue
            res_tmp['job status'] = f'ok'
            run_res.append(res_tmp)
        return run_res
