select * from Player;
create table PlayerCopy as select * from Player;
select * from PlayerCopy;


DO $$
DECLARE
    match_id    PlayerCopy.match_id%TYPE;
    player_name PlayerCopy.player_name%TYPE;
	ct_kd       PlayerCopy.ct_kd%TYPE;
    t_kd        PlayerCopy.t_kd%TYPE;


BEGIN
    match_id := 777;
    player_name := 'PName';
	ct_kd := '22-';
    t_kd := '11-';

    FOR counter IN 1..10
        LOOP
            INSERT INTO PlayerCopy(match_id, player_name, ct_kd,t_kd)
            VALUES (match_id, player_name || counter, ct_kd || counter, t_kd || counter);
        END LOOP;
END;
$$