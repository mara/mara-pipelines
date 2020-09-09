-- a data point for a run time chart

DROP TYPE IF EXISTS pg_temp.RUN_STAT CASCADE;

CREATE TYPE pg_temp.RUN_STAT AS (
  run_id INTEGER, -- for debugging
  start_time TIMESTAMPTZ, -- either the start time of the node or the first of its child nodes
  node_name TEXT, -- the name of the node that is run
  node_run JSONB, -- data about the run of the node itself
  child_names TEXT[], -- names of all child nodes
  child_runs JSONB[] -- data about runs of the top child nodes
  );

DROP FUNCTION IF EXISTS pg_temp.node_run_times(PATH TEXT[]);

CREATE FUNCTION pg_temp.node_run_times(path TEXT[])
  RETURNS SETOF pg_temp.RUN_STAT AS
$$
WITH node_runs AS ( -- runs of the node itself
  SELECT run_id,
         start_time,
         jsonb_build_object(
             'succeeded', succeeded,
             'duration', round((extract(EPOCH FROM end_time - start_time)) :: NUMERIC, 2)) AS data

  FROM data_integration_node_run
  WHERE node_path = path
    AND succeeded IS NOT NULL),

     top_children AS ( -- top top x children by average run time
       SELECT avg(extract(EPOCH FROM end_time - start_time)) AS average_duration,
              node_path [ array_upper(node_path, 1)]         AS node_name,
              node_path

       FROM data_integration_node_run
       WHERE array_length(node_path, 1) = coalesce(array_length(path, 1), 0) + 1
         AND node_path [ 0 : coalesce(array_length(path, 1), 0)] = path
         AND succeeded IS NOT NULL
       GROUP BY node_path
       ORDER BY 1 DESC
       LIMIT 8),

     top_children_ranked AS ( -- top children with rank by average duration
       SELECT *,
              row_number()
                  OVER (
                    ORDER BY average_duration DESC ) AS rank
       FROM top_children),

     top_child_runs AS ( -- the actual runs of the top x child nodes
       SELECT run_id,
              node_name,
              jsonb_build_object(
                  'duration', round((extract(EPOCH FROM end_time - start_time)) :: NUMERIC, 2),
                  'succeeded', succeeded) AS data,
              min(start_time)
                  OVER (
                    PARTITION BY run_id ) AS first_start_time
       FROM data_integration_node_run
              JOIN top_children
                   USING (node_path)
       ORDER BY node_path, run_id),

     all_runs AS ( -- all runs of parent node and children combined
       SELECT DISTINCT run_id,
                       coalesce(node_runs.start_time, first_start_time) AS start_time
       FROM node_runs
              FULL OUTER JOIN top_child_runs
                              USING (run_id)
       ORDER BY run_id)

SELECT all_runs.run_id,
       min(all_runs.start_time)                       AS time,

       coalesce(path [ array_upper(path, 1)], 'root') AS node_name,
       (SELECT data
        FROM node_runs
        WHERE node_runs.run_id = all_runs.run_id)     AS node_run,

       (SELECT array_agg(node_name
                         ORDER BY rank)
        FROM top_children_ranked)                     AS child_names,

       CASE
         WHEN exists(SELECT 1
                     FROM top_children_ranked)
           THEN
           array_agg(top_child_runs.data
                     ORDER BY top_children_ranked.rank)
         ELSE NULL END                                AS child_runs

FROM all_runs
       LEFT JOIN top_children_ranked
                 ON TRUE
       LEFT JOIN top_child_runs
                 ON top_child_runs.node_name = top_children_ranked.node_name
                   AND top_child_runs.run_id = all_runs.run_id
GROUP BY all_runs.run_id


ORDER BY all_runs.run_id;

$$
  LANGUAGE SQL;

-- SELECT *
-- FROM pg_temp.node_run_times(ARRAY [] :: TEXT []);

