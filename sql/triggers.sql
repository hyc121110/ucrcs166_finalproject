CREATE OR REPLACE LANGUAGE plpgsql;

CREATE SEQUENCE plane_no_seq START WITH 1;
CREATE SEQUENCE pilot_no_seq START WITH 1;
CREATE SEQUENCE flight_no_seq START WITH 1;
CREATE SEQUENCE flight_info_no_seq START WITH 1;
CREATE SEQUENCE technician_no_seq START WITH 1;
CREATE SEQUENCE reservation_no_seq START WITH 1;
--Create a proceduce that will return next value of the aforementioned sequence. Use function nextval('part_number_seq') to get the next value from the sequence. Use the following syntax to create your procedure.

--plane trigger
CREATE OR REPLACE FUNCTION plane_nextval()
RETURNS "trigger" AS
$BODY$
  BEGIN
  perform setval('plane_no_seq', (SELECT MAX(id) FROM Plane));
  NEW.id := nextval('plane_no_seq');
  RETURN NEW;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;

--pilot trigger
CREATE OR REPLACE FUNCTION pilot_nextval()
RETURNS "trigger" AS
$BODY$
  BEGIN
  perform setval('pilot_no_seq', (SELECT MAX(id) FROM Pilot));
  NEW.id := nextval('pilot_no_seq');
  RETURN NEW;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;

--Flight trigger
CREATE OR REPLACE FUNCTION flight_nextval()
RETURNS "trigger" AS
$BODY$
  BEGIN
  perform setval('flight_no_seq', (SELECT MAX(fnum) FROM Flight));
  NEW.fnum := nextval('flight_no_seq');
  RETURN NEW;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;

--Flight Info trigger
CREATE OR REPLACE FUNCTION flight_info_nextval()
RETURNS "trigger" AS
$BODY$
  BEGIN
  perform setval('flight_info_no_seq', (SELECT MAX(fiid) FROM FlightInfo));
  NEW.fiid := nextval('flight_info_no_seq');
  RETURN NEW;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;

--Technician trigger
CREATE OR REPLACE FUNCTION technician_nextval()
RETURNS "trigger" AS
$BODY$
  BEGIN
  perform setval('technician_no_seq', (SELECT MAX(id) FROM Technician));
  NEW.id := nextval('technician_no_seq');
  RETURN NEW;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE;

--Reservation trigger
CREATE OR REPLACE FUNCTION reservation_nextval()
RETURNS "trigger" AS
$BODY$
  BEGIN
  perform setval('reservation_no_seq', (SELECT MAX(rnum) FROM Reservation));
  NEW.rnum := nextval('reservation_no_seq');
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
CREATE TRIGGER trig_flight BEFORE INSERT
ON Flight FOR EACH ROW
EXECUTE PROCEDURE flight_nextval();
CREATE TRIGGER trig_flight_info BEFORE INSERT
ON FlightInfo FOR EACH ROW
EXECUTE PROCEDURE flight_info_nextval();
CREATE TRIGGER trig_technician BEFORE INSERT
ON Technician FOR EACH ROW
EXECUTE PROCEDURE technician_nextval();
CREATE TRIGGER trig_reservation BEFORE INSERT
ON Reservation FOR EACH ROW
EXECUTE PROCEDURE reservation_nextval();
