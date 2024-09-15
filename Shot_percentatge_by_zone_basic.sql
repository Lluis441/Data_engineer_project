CREATE TABLE data AS

SELECT
		p.player_name
		,t.full_name as team_name
		,shot_zone_basic
		,p.gp as games_played
		, count(*)  as shot_attempts
		,SUM(CASE WHEN shot_made_flag = 1 THEN 1 ELSE 0 END) AS shots_made
		,SUM(CASE WHEN shot_made_flag = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS shooting_percentage
		,s.points_per_shot as points_per_shot
		,pp.photo_url as player_photo
		,tl.primary_logo_url as team_logo
	
FROM
	NBA_FGA s
JOIN 
	players p on s.player_id=p.player_id
LEFT JOIN
	players_photo pp on pp.player_id=p.player_id
JOIN
	teams t on t.id=s.team_id
LEFT JOIN 
	teams_logos tl on tl.id=t.id

WHERE 1=1
	--and p.player_name='Pascal Siakam'

GROUP BY
	p.player_name, shot_zone_basic, t.id

HAVING 1=1
	and shot_attempts >20

ORDER BY 
 SUM(CASE WHEN shot_made_flag = 1 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) DESC
 
