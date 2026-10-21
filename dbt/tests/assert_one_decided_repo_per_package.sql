-- A package never has two accepted (or auto_exact) repositories.
SELECT package_key
FROM {{ ref('map_package_repo') }}
WHERE status IN ('auto_exact', 'accepted')
GROUP BY package_key
HAVING COUNT(*) > 1
