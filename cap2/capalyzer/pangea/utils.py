from pangea_api import (
    Knex,
    User,
    Organization,
    SampleAnalysisResultField,
)


def get_pangea_group(org_name, grp_name, email=None, password=None, endpoint='https://pangea.gimmebio.com'):
    knex = Knex()
    if email and password:
        User(knex, email, password).login()
    org = Organization(knex, org_name).get()
    grp = org.sample_group(grp_name).get()
    return grp
