select *
from {{ ref('features') }} limit 100