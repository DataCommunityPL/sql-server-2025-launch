use SQLServer2025Launch
 
go
INSERT INTO dbo.Sensors (SensorName, Location, Model, InstallDate)
VALUES 
('TempSensor-A1', 'Server Room', 'TS-1000', '2025-05-01'),
('TempSensor-B2', 'Warehouse', 'TS-2000', '2025-06-15'),
('TempSensor-C3', 'Office 1', 'TS-1000', '2025-07-20'),
('TempSensor-D4', 'Cold Storage', 'TS-3000', '2025-08-10'),
('TempSensor-E5', 'Lobby', 'TS-2000', '2025-09-01');
go
INSERT INTO dbo.SensorTempState (SensorID, LastTemp)
SELECT SensorID,
    CASE SensorName
        WHEN 'TempSensor-A1' THEN 20.00
        WHEN 'TempSensor-B2' THEN 19.50
        WHEN 'TempSensor-C3' THEN 21.00
        WHEN 'TempSensor-E5' THEN 22.00
        ELSE NULL
    END
FROM dbo.Sensors
WHERE SensorName IN ('TempSensor-A1', 'TempSensor-B2', 'TempSensor-C3', 'TempSensor-E5');

/*
delete from dbo.TemperatureReadings
delete from dbo.SensorTempState
delete from dbo.Sensors
*/

select * from dbo.Sensors

select * from dbo.SensorTempState

declare @wait nvarchar(20)

DECLARE @i INT = 0;
WHILE @i < 10000 -- Liczba pomiarów
BEGIN
    DECLARE @SensorID INT;
    DECLARE @Temperature DECIMAL(5,2);
    DECLARE @SensorName NVARCHAR(100);

    SELECT TOP 1 
        @SensorID = s.SensorID,
        @SensorName = s.SensorName
    FROM Sensors s
    ORDER BY NEWID(); -- losowy sensor

    IF @SensorName IN ('TempSensor-A1', 'TempSensor-B2', 'TempSensor-C3')
    BEGIN
        -- Delikatnie rosnąca temperatura
        DECLARE @LastTemp DECIMAL(5,2);
        SELECT @LastTemp = LastTemp FROM SensorTempState WHERE SensorID = @SensorID;

        SET @Temperature = @LastTemp + CAST(RAND() * 0.15 + 0.01 AS DECIMAL(5,2));

        -- Update stanu
        UPDATE dbo.SensorTempState SET LastTemp = @Temperature WHERE SensorID = @SensorID;
    END
    ELSE IF @SensorName = 'TempSensor-D4'
    BEGIN
        -- Losowa temperatura -5 do -3
        SET @Temperature = CAST(-5 + (RAND() * 2) AS DECIMAL(5,2));
    END
    ELSE IF @SensorName = 'TempSensor-E5'
    BEGIN
        DECLARE @LastTempE DECIMAL(5,2);
        SELECT @LastTempE = LastTemp FROM SensorTempState WHERE SensorID = @SensorID;

        -- Oscylacja w zakresie 21-23
        SET @Temperature = @LastTempE + CAST((RAND() - 0.5) * 0.4 AS DECIMAL(5,2));
        SET @Temperature = IIF(@Temperature < 21, 21, IIF(@Temperature > 23, 23, @Temperature));

        UPDATE dbo.SensorTempState SET LastTemp = @Temperature WHERE SensorID = @SensorID;
    END

    -- Wstawiamy pomiar
    INSERT INTO dbo.TemperatureReadings (SensorID, TemperatureCelsius)
    VALUES (@SensorID, @Temperature);

    -- Czekamy kilka sekund
	set @wait = '00:00:' + CAST(CAST(RAND() * 5 AS INT) AS VARCHAR)
    WAITFOR DELAY @wait;

    SET @i = @i + 1;
END


UPDATE dbo.TemperatureReadings
SET Sensorid=SensorId