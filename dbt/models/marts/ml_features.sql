with features as (
    select * from {{ ref('int_aq_features') }}
)

select * from features
