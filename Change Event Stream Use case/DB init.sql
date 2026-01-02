create database SQLServer2025Launch
go
use SQLServer2025Launch
go

CREATE TABLE dbo.Sensors (
    SensorID INT IDENTITY(1,1) PRIMARY KEY,
    SensorName NVARCHAR(100) NOT NULL,
    Location NVARCHAR(100),
    Model NVARCHAR(50),
    InstallDate DATE,
    IsActive BIT DEFAULT 1
);
go
CREATE TABLE dbo.SensorTempState (
    SensorID INT PRIMARY KEY,
    LastTemp DECIMAL(5,2)
);
go
CREATE TABLE dbo.TemperatureReadings (
    ReadingID BIGINT IDENTITY(1,1) PRIMARY KEY,
    SensorID INT NOT NULL FOREIGN KEY REFERENCES Sensors(SensorID),
    TemperatureCelsius DECIMAL(5,2),
    RecordedAt DATETIME2 DEFAULT SYSUTCDATETIME()
);
go
