CREATE EXTENSION plpython3u ;

CREATE or REPLACE FUNCTION pymax (a integer, b integer)
  RETURNS integer
AS $$
  rv = plpy.execute ('SELECT * FROM "My".cities', 5)
  print (rv)
  return (len (rv))
  if a > b:
    return a
  return b
$$ LANGUAGE plpython3u;

select pymax (5, 10) ;
