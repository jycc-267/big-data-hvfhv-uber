-- Manually create and insert rows into the HBase table jycchien_carrier
create 'jycchien_carrier', 'info'
  put 'jycchien_carrier', 'Juno', 'info:carrier', 'Juno'
  put 'jycchien_carrier', 'Uber', 'info:carrier', 'Uber'
  put 'jycchien_carrier', 'Via', 'info:carrier', 'Via'
  put 'jycchien_carrier', 'Lyft', 'info:carrier', 'Lyft'

create 'jycchien_license', 'info'
  put 'jycchien_license', 'HV0002', 'info:carrier', 'Juno'
  put 'jycchien_license', 'HV0003', 'info:carrier', 'Uber'
  put 'jycchien_license', 'HV0004', 'info:carrier', 'Via'
  put 'jycchien_license', 'HV0005', 'info:carrier', 'Lyft'


-- See the spark-shell script
  create 'jycchien_zone', 'info'
  create 'jycchien_zone_map', 'info'
  create 'jycchien_hours', 'info'
