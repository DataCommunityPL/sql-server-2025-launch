
use SQLServer2025Launch
 
go

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'H@rd2Gue$$P@$$w0rd'


CREATE DATABASE SCOPED CREDENTIAL SqlCesCredential
WITH
  IDENTITY = 'SHARED ACCESS SIGNATURE',
  SECRET = 'SharedAccessSignature sr=https%3a%2f%2fbialykruk2008.servicebus.windows.net%2feh1&sig=0hj8HBfi3EdAjC0y7DBXB%2bb14O9gQzlVi4mCuI7l1cU%3d&se=1781706269&skn=ces-demo-policy'

  
 ALTER DATABASE SCOPED CONFIGURATION SET PREVIEW_FEATURES = ON;
GO


EXEC [sys].[sp_enable_event_stream]


  	
SELECT * FROM sys.databases WHERE is_event_stream_enabled = 1


EXEC sys.sp_create_event_stream_group
  @stream_group_name      = 'SqlCesGroup',
  @destination_location   = 'bialykruk2008.servicebus.windows.net/eh1',
  @destination_credential = SqlCesCredential,
  @destination_type       = 'AzureEventHubsAmqp'

 
EXEC sys.sp_add_object_to_event_stream_group
  @stream_group_name = 'SqlCesGroup',
  @object_name = 'dbo.Sensors',
  @include_old_values = 0,      -- do not include old values on updates/deletes
  @include_all_columns = 1      -- include all columns even if unchanged
 
 
EXEC sys.sp_add_object_to_event_stream_group
  @stream_group_name = 'SqlCesGroup',
  @object_name = 'dbo.TemperatureReadings',
  @include_old_values = 0,      -- do not include old values on updates/deletes
  @include_all_columns = 1      -- include all columns even if unchanged
 

 



EXEC sp_help_change_feed_table @source_schema = 'dbo', @source_name = 'Sensors'
EXEC sp_help_change_feed_table @source_schema = 'dbo', @source_name = 'TemperatureReadings'
 




 EXEC [sys].[sp_disable_event_stream]