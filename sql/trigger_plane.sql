CREATE OR REPLACE LANGUAGE plpgsql;

CREATE SEQUENCE plane_no_seq START WITH 67;
CREATE SEQUENCE pilot_no_seq START WITH 250;
--Create a proceduce that will return next value of the aforementioned sequence. Use function nextval('part_number_seq') to get the next value from the sequence. Use the following syntax to create your procedure.

CREATE OR REPLACE FUNCTION plane_nextval()
RETURNS "trigger" AS
$BODY$
  BEGIN
  NEW.id := nextval('plane_no_seq');
  RETURN NEW;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;

CREATE OR REPLACE FUNCTION pilot_nextval()
RETURNS "trigger" AS
$BODY$
  BEGIN
  NEW.id := nextval('pilot_no_seq');
  RETURN NEW;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;

--Use the following syntax to create a trigger calling the procedure upon insertion of the new record
CREATE TRIGGER trig_plane BEFORE INSERT
ON Plane FOR EACH ROW
EXECUTE PROCEDURE plane_nextval();
CREATE TRIGGER trig_pilot BEFORE INSERT
ON Pilot FOR EACH ROW
EXECUTE PROCEDURE pilot_nextval();