DROP TABLE IF EXISTS provider_power_daily;

CREATE TABLE provider_power_daily (
  provider VARCHAR(20) NOT NULL,
  date DATE,
  "avg(rawBytePower)" DOUBLE PRECISION,
  "avg(qualityAdjPower)" DOUBLE PRECISION
);

