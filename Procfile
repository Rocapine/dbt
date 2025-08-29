release: dbt deps --profiles-dir ${DBT_PROFILES_DIR:-profiles} --project-dir .
dbt: dbt build --profiles-dir ${DBT_PROFILES_DIR:-profiles} --project-dir . --target ${DBT_TARGET:-prod}
