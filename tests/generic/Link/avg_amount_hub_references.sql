{% test avg_amount_hub_references(model) %}

{% set links_query %}
    -- 1. Get all distinct links and count their foreign hashkeys.
    SELECT DISTINCT foreign_hashkeys
    FROM {{ ref('link_metadata') }}
    WHERE foreign_hashkeys IS NOT NULL
    AND invocation_id = {{ get_latest_invocation_id('link_metadata') }}
{% endset %}

{% set results = run_query(links_query) %}

{% set total_links = 0 %}
-- We use a list to get around the variable scoping issue.
{% set total_fks_list = [0] %}
{% set average_fks = 0 %}
{% set test_status = 'pass' %}
{% set message = '' %}

-- 2. Iterate over the results to count total links and foreign keys.
{% if execute and results and results.rows %}
    {% set total_links = results.rows | length %}
    {% for row in results.rows %}
        {% set foreign_hashkeys = row[0] | trim %}
        {% set fk_count = foreign_hashkeys.split(', ') | map('trim') | length %}

        -- Increment the total_fks value stored in the list.
        {% set new_total_fks = total_fks_list[0] + fk_count %}
        {% set dummy = total_fks_list.pop() %}
        {% do total_fks_list.append(new_total_fks) %}

        {{ log('fk_count ' ~ fk_count, info=true) }}
        {{ log('total_fks ' ~ total_fks_list[0], info=true) }}
    {% endfor %}
    {% set total_fks = total_fks_list[0] %}
{% endif %}

-- 3. Calculate the average and determine the test status.
{% if total_links > 0 %}
    {% set average_fks = ((total_fks | float) / total_links) | round(2) %}
    {% if average_fks < 2 %}
        {% set test_status = 'error' %}
        {% set message = 'ERROR: The average number of foreign hashkeys per link is ' ~ average_fks ~ ', which is less than 2. This may indicate a broken Unit of Work.' %}
    {% elif average_fks == 2 %}
        {% set test_status = 'warn' %}
        {% set message = 'WARNING: The average number of foreign hashkeys per link is exactly 2. Consider whether all links are connecting more than two hubs.' %}
    {% endif %}
{% else %}
    {% set test_status = 'error' %}
    {% set message = 'ERROR: No links found in metadata. Cannot calculate average.' %}
{% endif %}

-- DEBUG: Log the calculated average to the terminal
{{ log("Calculated average foreign keys: " ~ average_fks, info=true) }}

-- 4. Generate the final SQL query based on the test status.
{% if test_status == 'error' or test_status == 'warn' %}
    SELECT DISTINCT
        '{{ message }}' AS test_message,
        '{{ test_status }}' AS test_status,
        {{ average_fks }} AS average_fks
    FROM {{ ref('link_metadata') }}
    WHERE 1=1
{% else %}
    -- Return an empty result set if the test passes.
    SELECT
        NULL AS test_message,
        NULL AS test_status,
        NULL AS average_fks
    FROM {{ ref('link_metadata') }}
    WHERE 1=0
{% endif %}


{% endtest %}