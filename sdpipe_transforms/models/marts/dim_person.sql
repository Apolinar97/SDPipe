{{ config(unique_key='person_key') }}

WITH distinct_persons AS (
    SELECT DISTINCT
        person_role,
        person_injury_level,
        person_vehicle_type,
        vehicle_type,
        vehicle_make,
        vehicle_model
    FROM {{ ref('int_collisions_details') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'person_role',
        'person_injury_level',
        'person_vehicle_type',
        'vehicle_type',
        'vehicle_make',
        'vehicle_model'
    ]) }} AS person_key,
    person_role,
    person_injury_level,
    person_vehicle_type,
    vehicle_type,
    vehicle_make,
    vehicle_model
FROM distinct_persons
