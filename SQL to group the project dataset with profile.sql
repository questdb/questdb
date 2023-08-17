SELECT project_id, profile_id, COUNT(*) AS total_projects
FROM projects
GROUP BY project_id, profile_id;
