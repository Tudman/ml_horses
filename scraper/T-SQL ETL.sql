

-- Insert Jockey
-- Type-1 and only 1 field, name.
;WITH jky (JockeyName, ETLRunID) AS (
SELECT LEFT(Jockey, CHARINDEX('(', Jockey) - 2),
	MAX(ETLRunID)
FROM Stage.Runner
GROUP BY LEFT(Jockey, CHARINDEX('(', Jockey) - 2))

INSERT INTO dw.Jockey (
	JockeyName, Created, CreatedETLRunKey)
SELECT jky.JockeyName, GETDATE(), ETLRunID
FROM jky
WHERE jky.JockeyName NOT IN (
	SELECT JockeyName 
	FROM dw.Jockey)
ORDER BY jky.JockeyName



-- Source for Horse
-- make sure dates of birth correct
UPDATE Stage.Runner SET
	DateOfBirth = c.DateOfBirth
FROM (SELECT HorsePageURL as HPU, DateOfBirth
	FROM Stage.Runner rnr
	WHERE rnr.DateOfBirth <> '1900-01-01') as c
WHERE c.HPU = HorsePageURL
;
GO



WITH hs (HorseName, TrainerName, DateOfBirth, EffectiveFromETLRunKey, 
	EffectiveFrom, EffectiveToETLRunKey, EffectiveTo) AS (
SELECT UPPER(rnr.Horse),
	UPPER(rnr.Trainer),
	rnr.DateOfBirth,
	MIN(rnr.ETLRunID) OVER (PARTITION BY UPPER(rnr.Horse), UPPER(rnr.Trainer), rnr.DateOfBirth)AS EffectiveFromETLRunKey,
	MIN(rm.MeetingDate) OVER (PARTITION BY UPPER(rnr.Horse), UPPER(rnr.Trainer), rnr.DateOfBirth) AS EffectiveFrom,
	MAX(rnr.ETLRunID) OVER (PARTITION BY UPPER(rnr.Horse), UPPER(rnr.Trainer), rnr.DateOfBirth) AS EffectiveToETLRunKey,
	MAX(rm.MeetingDate) OVER (PARTITION BY UPPER(rnr.Horse), UPPER(rnr.Trainer), rnr.DateOfBirth) AS EffectiveTo
FROM Stage.Runner rnr
	INNER JOIN Stage.Race r
		ON r.RaceID = rnr.RaceID
	INNER JOIN Stage.RaceMeeting rm
		ON r.RaceMeetingID = rm.RaceMeetingID
)

, hs1 (HorseName, TrainerName, DateOfBirth, EffectiveFromETLRunKey, 
	EffectiveFrom, EffectiveToETLRunKey, EffectiveTo) AS (
SELECT HorseName, TrainerName, DateOfBirth, EffectiveFromETLRunKey, 
	EffectiveFrom, EffectiveToETLRunKey, EffectiveTo
FROM hs
GROUP BY HorseName, TrainerName, DateOfBirth, EffectiveFromETLRunKey, 
	EffectiveFrom, EffectiveToETLRunKey, EffectiveTo
)

INSERT INTO dw.Horse (
	DateOfBirthCalendarKey, HorseName, TrainerName, DateOfBirth,
	[Source], EffectiveFrom, EffectiveFromETLRunKey, EffectiveTo,
	EffectiveToETLRunKey, IsCurrent)
SELECT CAST(CONVERT(CHAR(8), DateOfBirth, 112) AS INT) AS DateOfBirthCalendarKey, 
	HorseName, TrainerName, DateOfBirth, 'Racenet', EffectiveFrom, EffectiveFromETLRunKey, 
	EffectiveTo, EffectiveToETLRunKey, 0
FROM hs1
ORDER BY HorseName, TrainerName, DateOfBirth, EffectiveFromETLRunKey, 
	EffectiveFrom, EffectiveToETLRunKey, EffectiveTo









-- Source for dw.Race
INSERT INTO dw.Race (
	RaceCalendarKey, RaceDate, StartTime, [State], Track, RaceNumber, RaceName,
	RaceDistanceMeters, IsTrial, RailPosition, TrackCondition, TrackType, Weather,
	Penetrometer, WinningTimeSeconds, LastSplitTime, ResultsLastPublished,
	RaceComments, [Source], PrizeMoney, Created, CreatedETLRunKey, Updated,
	UpdatedETLRunKey)
SELECT 
	CAST(CONVERT(char(8), rm.MeetingDate, 112) AS int) AS RaceCalendarKey,
	CAST(rm.MeetingDate AS Date) AS RaceDate,
	CAST(r.RaceTime AS Time) AS StartTime,	
	rm.[State] AS [State],
	rm.Location AS Track,
	r.RaceNumber AS RaceNumber,
	r.RaceName AS RaceName,
	CAST(LEFT(r.RaceDistance, LEN(LTRIM(RTRIM(r.RaceDistance)))-1) AS int) AS RaceDistanceMeters,
	rm.IsTrial AS IsTrial,
	rm.RailPosition AS RailPosition,
	rm.TrackCondition AS TrackCondition,
	rm.TrackType AS TrackType,
	rm.Weather AS Weather,
	rm.Penetrometer AS Penetrometer,
	CAST(LEFT(r.WinningTime, CHARINDEX(':', r.WinningTime) -1) AS decimal) * 60
		+ CAST(SUBSTRING(r.WinningTime, CHARINDEX(':', r.WinningTime) + 1, 5) AS decimal(6,2)) AS WinningTimeSeconds,
	-1 AS LastSplitTime,
	rm.ResultsLastPublished AS ResultsLastPublished,
	'' AS RaceComments,	--rm.Comments
	r.[Source] AS [Source],
	r.OfficialComments AS PrizeMoney,
	GETDATE() AS Created,
	r.ETLRunID AS CreatedETLRunKey,
	'1-1-1900' AS Updated,
	-1 AS UpdatedETLRunKey
FROM Stage.RaceMeeting rm
	INNER JOIN Stage.Race r
		ON r.RaceMeetingId = rm.RaceMeetingID




-- Source for RaceResult
INSERT INTO dw.RaceResult (
	RaceKey, JockeyKey, RaceCalendarKey, HorseKey, AgeOnRaceDayDays,
	FinishPositionKey, RaceTimeSeconds, StartingBarrier, StartingPrice,
	WeightKg, JockeysAllowance, Penalty, [Source], Created, CreatedETLRunKey,
	Updated, UpdatedETLRunKey)
SELECT
	ISNULL(rdim.RaceKey, -1) AS RaceKey,
	ISNULL(j.JockeyKey, -1) AS JockeyKey,
	CAST(CONVERT(char(8), rm.MeetingDate, 112) AS int) AS RaceCalendarKey,
	ISNULL(h.HorseKey, -1) AS HorseKey,
	DATEDIFF(d, rnr.DateOfBirth, rm.MeetingDate) AS AgeOnRaceDayDays,
	ISNULL(fp.FinishPositionKey, -1) AS FinishPositionKey,
	rnr.RaceTime AS RaceTimeSeconds,
	ISNULL(Barrier, -1) AS StartingBarrier,
	CAST(StartingPrice AS money) AS StartingPrice,
	CAST(rnr.[Weight] AS decimal(5,1)) AS WeightKg,
	0 AS JockeysAllowance, 
	rnr.Penalty AS Penalty,
	rnr.[Source] AS [Source],
	GETDATE() AS Created,
	rnr.ETLRunID AS CreatedETLRunKey,
	'1-1-1900' AS Updated,
	-1 AS UpdatedETLRunKey
FROM Stage.Runner rnr
	INNER JOIN Stage.Race r
		ON rnr.RaceID = r.RaceID
	INNER JOIN Stage.RaceMeeting rm
		ON r.RaceMeetingID = rm.RaceMeetingID
	-- Lookups to Dimensions
	LEFT JOIN dw.FinishPosition fp
		ON fp.FinishPosition = rnr.Result
	LEFT JOIN dw.Race rdim
		ON rdim.RaceDate = rm.MeetingDate
		AND rdim.StartTime = CAST(r.RaceTime AS time)
		AND rdim.Track = rm.Location
		AND rdim.RaceNumber = r.RaceNumber
	LEFT JOIN dw.Jockey j
		ON LEFT(rnr.Jockey, CHARINDEX('(', rnr.Jockey) - 2) = j.JockeyName
	LEFT JOIN dw.Horse h
		ON UPPER(rnr.Horse) = h.HorseName
		AND UPPER(rnr.Trainer) = h.TrainerName
		AND rnr.DateOfBirth = h.DateOfBirth
		AND rm.MeetingDate >= h.EffectiveFrom
		AND rm.MeetingDate <= h.EffectiveTo
