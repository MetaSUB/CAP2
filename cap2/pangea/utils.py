import luigi
from luigi.configuration import get_config


def set_config(endpoint, email, password,
               org_name=None, grp_name=None, work_order_name=None,
               name_is_uuid=False, upload_allowed=True, download_only=False,
               data_kind='short_read'):
    get_config().set('pangea', 'pangea_endpoint', endpoint)
    get_config().set('pangea', 'user', email)
    get_config().set('pangea', 'password', password)

    get_config().set('pangea', 'org_name', org_name if org_name else '')
    get_config().set('pangea', 'grp_name', grp_name if grp_name else '')
    get_config().set('pangea', 'work_order_name', work_order_name if work_order_name else '')
    

    get_config().set('pangea', 'name_is_uuid', 'name_is_uuid' if name_is_uuid else '')
    get_config().set('pangea', 'upload_allowed', 'upload_allowed' if upload_allowed else '')
    get_config().set('pangea', 'download_only', 'download_only' if download_only else '')
    get_config().set('pangea', 'data_kind', data_kind)

    
