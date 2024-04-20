USE [HistoricalPriceDB]
GO

/****** Object:  Table [dbo].[CEF_price_nav_history]    Script Date: 4/20/2024 12:27:41 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[CEF_price_nav_history](
	[Symbol] [nvarchar](10) NOT NULL,
	[MFQSSym] [nvarchar](10) NOT NULL,
	[Date] [date] NOT NULL,
	[OpenPx] [decimal](9, 2) NOT NULL,
	[HighPx] [decimal](9, 2) NOT NULL,
	[LowPx] [decimal](9, 2) NOT NULL,
	[ClosePx] [decimal](9, 2) NOT NULL,
	[NAV] [decimal](9, 2) NOT NULL
) ON [PRIMARY]
GO

