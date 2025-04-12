USE [HistoricalPriceDB]
GO

/****** Object:  Table [dbo].[TestHistoricalData]    Script Date: 4/12/2025 1:15:05 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[TestHistoricalData](
	[Symbol] [nvarchar](10) NULL,
	[QuoteId] [int] NULL,
	[Date] [date] NULL,
	[OpenPx] [decimal](9, 2) NULL,
	[HighPx] [decimal](9, 2) NULL,
	[LowPx] [decimal](9, 2) NULL,
	[ClosePx] [decimal](9, 2) NULL,
	[Volume] [bigint] NULL,
	[RngPct] [decimal](5, 4) NULL,
	[Bars] [int] NULL
) ON [PRIMARY]
GO


