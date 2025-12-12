from typing import Literal

class TickerConfig:
    
    IdLiteral = Literal["ticker", "isin", "performance_id", "security_id"]
    
    SecurityTypeLiteral = Literal["fund", "index", "etf", "stock"]
    
    BooleanLiteral = Literal[True, False]
    
    CountryLiteral = Literal[
        "United States", "United Kingdom", "Canada", "Australia", "Japan", "Germany",
        "France", "Switzerland", "Netherlands", "Sweden", "Norway", "Denmark",
        "Finland", "Ireland", "Austria", "Belgium", "Spain", "Italy", "Portugal",
        "Greece", "Poland", "Czech Republic", "Hungary", "Romania", "Bulgaria",
        "Croatia", "Slovenia", "Slovakia", "Estonia", "Latvia", "Lithuania",
        "Luxembourg", "Malta", "Cyprus", "Iceland", "China", "Hong Kong", "Taiwan",
        "South Korea", "Singapore", "India", "Thailand", "Malaysia", "Indonesia",
        "Philippines", "Vietnam", "Pakistan", "Bangladesh", "Sri Lanka", "Nepal",
        "Brazil", "Mexico", "Argentina", "Chile", "Colombia", "Peru", "Uruguay",
        "Bolivia", "Ecuador", "Venezuela", "South Africa", "Egypt", "Nigeria",
        "Kenya", "Ghana", "Morocco", "Tunisia", "Zambia", "Zimbabwe", "Tanzania",
        "Namibia", "Botswana", "Malawi", "Mauritius", "Ivory Coast", "Eswatini",
        "Turkey", "Israel", "Saudi Arabia", "United Arab Emirates", "Qatar",
        "Kuwait", "Bahrain", "Oman", "Jordan", "Lebanon", "Iraq", "Iran",
        "Palestine", "Kazakhstan", "Russia", "Ukraine", "Bosnia and Herzegovina",
        "Serbia", "Montenegro", "North Macedonia", "Armenia", "Bermuda", "Panama",
        "Jamaica", "Trinidad and Tobago"
    ]
    CountryIdLiteral = Literal[
        "USA", "GBR", "CAN", "AUS", "JPN", "DEU", "FRA", "CHE", "NLD", "SWE",
        "NOR", "DNK", "FIN", "IRL", "AUT", "BEL", "ESP", "ITA", "PRT", "GRC",
        "POL", "CZE", "HUN", "ROU", "BGR", "HRV", "SVN", "SVK", "EST", "LVA",
        "LTU", "LUX", "MLT", "CYP", "ISL", "CHN", "HKG", "TWN", "KOR", "SGP",
        "IND", "THA", "MYS", "IDN", "PHL", "VNM", "PAK", "BGD", "LKA", "NPL",
        "BRA", "MEX", "ARG", "CHL", "COL", "PER", "URY", "BOL", "ECU", "VEN",
        "ZAF", "EGY", "NGA", "KEN", "GHA", "MAR", "TUN", "ZMB", "ZWE", "TZA",
        "NAM", "BWA", "MWI", "MUS", "CIV", "SWZ", "TUR", "ISR", "SAU", "ARE",
        "QAT", "KWT", "BHR", "OMN", "JOR", "LBN", "IRQ", "IRN", "PSE", "KAZ",
        "RUS", "UKR", "BIH", "SRB", "MNE", "MKD", "ARM", "BMU", "PAN", "JAM",
        "TTO"
    ]
    ExchangeIdLiteral = Literal[
        "XDAR", "XNEP", "XMAU", "XBDA", "XLJU", "XMSW", "XPTY", "XJAM", "XBRV",
        "XBOT", "XMNX", "XBRA", "XPRA", "XCOL", "XBUD", "XDHA", "XSWA", "XNAM", "XZAG",
        "XTRN", "XBUE", "XSWX", "XBSE", "XRIS", "XARM", "XTAL", "XLIM", "XBOG", "XADS",
        "XDFM", "DIFX", "XLIT", "XMNT", "XMAL", "XLUX", "XCYS", "XMAE", "XKUW", "XBOL",
        "DSMD", "XGUA", "XQUI", "XBAH", "XSAU", "XMUS", "XICE", "XJSE", "XHEL", "XOSL",
        "XBEL", "BVCA", "XBEY", "XDUB", "XAMM", "XCAI", "XBRU", "XGHA", "XNAI", "XNSA",
        "XIQS", "XTEH", "XPAE", "XMAD", "XSGO", "XKAZ", "XCAS", "XLIS", "XTUN", "XATH",
        "XPHS", "PFTS", "UKEX", "XBLB", "XKAR", "BVMF", "XBUL", "XCSE", "XTKS", "XNGO",
        "XSAP", "XFKA", "XSTO", "XSAT", "XNGM", "XTAE", "XZIM", "XIST", "XWBO", "XKRX",
        "XLUS", "XSES", "XPAR", "XASX", "XNEC", "ROCO", "XTAI", "XMIL", "XHKG", "SZSC",
        "SHSC", "XBOM", "XNSE", "CHIX", "LTS", "AQSE", "XLON", "MISX", "XKLS", "XNAS",
        "PINX", "XNYS", "GREY", "XASE", "XOTC", "ARCX", "BATS", "XWAR", "HSTC", "XSTC",
        "XMEX", "XIDX", "XTSX", "XCNQ", "NEOE", "XTSE", "XBKK", "CEUX", "XAMS", "XSSC",
        "XSHG", "XSHE", "XSEC", "BJSE", "XMUN", "XBER", "XDUS", "XSTU", "XFRA", "XETR",
        "XHAM", "XHAN", "XMCE", "CHIA", "XDBC", "XGAT", "360T", "FRAA", "ETFP", "XATS",
        "XMAT", "ALXP", "XMON", "F", "XBRN", "XVTX", "XQMH", "XCBO", "OTCB", "OTCQ",
        "CBSX", "C2OX", "EAM", "XLDN", "BATE", "SWB", "BEEX", "SSOB", "MIQ", "XBES"
    ]
    ExchangeNameLiteral = Literal[
        "Dar es Salaam Stock Exchange", "Nepal Stock Exchange", "Mauritius Stock Exchange",
        "Bermuda Stock Exchange", "Ljubljana Stock Exchange (Slovenia)", "Swaziland Stock Exchange",
        "Port Moresby Stock Exchange", "Jamaica Stock Exchange", "BVRio Environmental Exchange",
        "Botswana Stock Exchange", "Minneapolis Grain Exchange", "Brasil Bolsa Balcão (B3)",
        "Prague Stock Exchange (Alternative)", "Colombia Stock Exchange", "Budapest Stock Exchange",
        "Dhaka Stock Exchange", "Eswatini Stock Exchange", "Namibia Stock Exchange", "Zagreb Stock Exchange",
        "Tirana Stock Exchange", "Buenos Aires Stock Exchange", "SIX Swiss Exchange", "Bucharest Stock Exchange",
        "Riga Stock Exchange", "Armenia Securities Exchange", "Tallinn Stock Exchange", "Lima Stock Exchange",
        "Bogota Stock Exchange", "Abu Dhabi Securities Exchange", "Dubai Financial Market", "NASDAQ Dubai",
        "Lithuania Stock Exchange", "Mongolia Stock Exchange", "Malaysia Derivatives Exchange", "Luxembourg Stock Exchange",
        "Cyprus Stock Exchange", "Cairo & Alexandria Stock Exchange", "Kuwait Stock Exchange", "Bolivia Stock Exchange",
        "Nasdaq Copenhagen (Dark)", "Guatemala Stock Exchange", "Quito Stock Exchange", "Bahrain Stock Exchange",
        "Saudi Exchange (Tadawul)", "Muscat Securities Market (Oman)", "Iceland Stock Exchange", "Johannesburg Stock Exchange",
        "Nasdaq Helsinki", "Oslo Børs", "Belgium Market (Euronext Brussels)", "Caracas Stock Exchange", "Beirut Stock Exchange",
        "Dublin Stock Exchange", "Amman Stock Exchange", "Egyptian Exchange", "Euronext Brussels", "Ghana Stock Exchange",
        "Nairobi Securities Exchange", "Nigeria Stock Exchange", "Iraq Stock Exchange", "Tehran Stock Exchange",
        "Palestine Securities Exchange", "Madrid Stock Exchange", "Santiago Stock Exchange", "Kazakhstan Stock Exchange",
        "Casablanca Stock Exchange", "Euronext Lisbon", "Tunis Stock Exchange", "Athens Stock Exchange", "Philippine Stock Exchange",
        "PFTS Ukraine Stock Exchange", "Ukraine Exchange", "Belgrade Stock Exchange", "Pakistan Stock Exchange (Karachi)",
        "B3 - Brasil Bolsa Balcão", "Bulgarian Stock Exchange", "Nasdaq Copenhagen", "Tokyo Stock Exchange", "Nagoya Stock Exchange",
        "Sapporo Securities Exchange", "Fukuoka Stock Exchange", "Nasdaq Stockholm", "Santiago Alternative Market",
        "Nordic Growth Market", "Tel Aviv Stock Exchange", "Zimbabwe Stock Exchange", "Borsa Istanbul", "Wiener Börse (Vienna)",
        "Korea Exchange", "Cboe US Equities (CXE)", "Singapore Exchange", "Euronext Paris", "Australian Securities Exchange",
        "Nagoya Stock Exchange (NEC)", "Romanian Commodities Exchange", "Taiwan Stock Exchange", "Borsa Italiana Milan",
        "Hong Kong Stock Exchange", "Shenzhen Stock Exchange", "Shanghai Stock Exchange", "Bombay Stock Exchange",
        "National Stock Exchange of India", "Cboe Europe (CHI-X)", "London Trading System", "Aquis Stock Exchange",
        "London Stock Exchange", "Moscow Exchange", "Bursa Malaysia", "NASDAQ", "OTC Pink Market", "New York Stock Exchange",
        "Grey Market (OTC)", "NYSE American", "OTC Markets", "NYSE Arca", "Cboe BATS", "Warsaw Stock Exchange",
        "Ho Chi Minh Stock Exchange (old code)", "Ho Chi Minh Stock Exchange", "Mexican Stock Exchange", "Indonesia Stock Exchange",
        "TSX Venture Exchange", "Canadian Securities Exchange", "Neo Exchange", "Toronto Stock Exchange", "Stock Exchange of Thailand",
        "Cboe Europe (UK)", "Euronext Amsterdam", "Shenzhen Stock Exchange", "Shanghai Stock Exchange", "Shenzhen Stock Exchange",
        "Securities Exchange Center (Thailand)", "Beijing Stock Exchange", "Munich Exchange", "Berlin Exchange", "Düsseldorf Stock Exchange",
        "Stuttgart Stock Exchange", "Frankfurt Stock Exchange", "Xetra (Deutsche Börse)", "Hamburg Stock Exchange", "Hannover Exchange",
        "Macedonia Stock Exchange", "Cboe Australia (CHIA)", "Dubai Gold & Commodities Exchange", "Global Access Trading",
        "360T FX Trading Platform", "Frankfurt Floor Trading", "ETF Platform", "ATS Market", "MAT Stock Exchange", "Euronext Access",
        "Montenegro Stock Exchange", "Frankfurt (unofficial code)", "Brunei Stock Exchange", "SIX Swiss Exchange (CHF market)",
        "Qatar Stock Exchange (QMH)", "Cboe Exchange", "OTC Bulletin Board", "OTCQB Venture Market", "Cboe Stock Exchange",
        "Cboe C2 Options Exchange", "Euronext Amsterdam MTF", "Cboe Europe (LDN)", "Cboe Europe (BATE)", "Stuttgart Stock Exchange (SWB)",
        "Beirut Electronic Exchange", "Sovereign Bond Market", "Borsa Italiana (MTF)", "Bermuda Electronic Securities"
    ]
    
    SectorLiteral = Literal[
        "Consumer Cyclical", "Consumer Defensive", "Real Estate",
        "Basic Materials", "Communication Services", "Financial Services",
        "Utilities", "Healthcare", "Technology", "Industrials", "Energy"
    ]
        
    