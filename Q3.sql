SELECT U.user_id, U.first_name, U.last_name
FROM USER1 U, SYZHAO.PUBLIC_USER_CURRENT_CITY C2, SYZHAO.PUBLIC_USER_HOMETOWN_CITY C1
WHERE U.user_id = C1.user_id AND U.user_id = C2.user_id
AND C1.hometown_city_id <> C2.current_city_id
ORDER BY U.user_id;