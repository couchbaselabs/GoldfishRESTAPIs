class s3Config:
    def __init__(self, access_key, secret_key, region, num_buckets, depth_level, num_folders_per_level,
                 num_files_per_level, num_rows_per_file=1, session_token=None, file_size=1024, max_file_size=10240,
                 file_format=['json', 'csv', 'tsv']):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.num_buckets = num_buckets
        self.depth_level = depth_level
        self.num_folders_per_level = num_folders_per_level
        self.num_files_per_level = num_files_per_level
        self.num_rows_per_file = num_rows_per_file
        self.session_token = session_token
        self.file_size = file_size
        self.max_file_size = max_file_size
        self.file_format = file_format
