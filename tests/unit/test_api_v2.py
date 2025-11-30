import pytest
import pandas as pd
from unittest.mock import Mock, patch, AsyncMock

from morningpy import get_financial_statement
from morningpy.extractor.security import FinancialStatementExtractor

@pytest.fixture
def fake_response():
    """Fixture contenant la réponse API mockée"""
    return {
        "_meta": {
            "companyId": "0C00004EP4",
            "statementType": "income-statement",
        },
        "columnDefs": [
            "Q1 2020", "Q2 2020", "Q3 2020", "Q4 2020", "Q1 2021",
            "Q2 2021", "Q3 2021", "Q4 2021", "Q1 2022", "Q2 2022",
            "Q3 2022", "Q4 2022", "Q1 2023", "Q2 2023", "Q3 2023",
            "Q4 2023", "Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024",
            "Q1 2025", "Q2 2025", "Q3 2025", "TTM"
        ],
        "rows": [
            {
                "parentId": "",
                "label": "IncomeStatement",
                "dataPointId": "IFIS000000",
                "order": 385,
                "subLevel": [
                    {
                        "parentId": "IFIS000000385",
                        "label": "Gross Profit",
                        "dataPointId": "IFIS000590",
                        "order": 386,
                        "datum": [
                            "_PO_", "_PO_", "_PO_", "_PO_", "_PO_",
                            2884.0, 3660.0, 4847.0, 5460.0, 4234.0,
                            5382.0, 5777.0, 4511.0, 4533.0, 4178.0,
                            4438.0, 3696.0, 4578.0, 4997.0, 4179.0,
                            3153.0, 3878.0, 5054.0, 16264.0
                        ],
                        "subLevel": [
                            {
                                "parentId": "IFIS000590386",
                                "label": "Total Revenue",
                                "dataPointId": "IFIS001170",
                                "order": 387,
                                "collapsed": True,
                                "datum": [
                                    "_PO_", "_PO_", "_PO_", "_PO_", "_PO_",
                                    11958.0, 13757.0, 17719.0, 18756.0, 16934.0,
                                    21454.0, 24318.0, 23329.0, 24927.0, 23350.0,
                                    25167.0, 21301.0, 25500.0, 25182.0, 25707.0,
                                    19335.0, 22496.0, 28095.0, 95633.0
                                ],
                                "subLevel": [
                                    {
                                        "parentId": "IFIS001170387",
                                        "label": "Business Revenue",
                                        "dataPointId": "IFIS001839",
                                        "order": 388,
                                        "collapsed": True,
                                        "datum": [
                                            "_PO_", "_PO_", "_PO_", "_PO_", "_PO_",
                                            11958.0, 13757.0, 17719.0, 18756.0, 16934.0,
                                            21454.0, 24318.0, 23329.0, 24927.0, 23350.0,
                                            25167.0, 21301.0, 25500.0, 25182.0, 25707.0,
                                            19335.0, 22496.0, 28095.0, 95633.0
                                        ]
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ],
        "footer": {
            "currency": "USD",
            "currencySymbol": "$",
            "orderOfMagnitude": "Million",
            "fiscalYearEndDate": "12-31"
        }
    }


@pytest.fixture
def expected_dataframe():
    """Fixture contenant le DataFrame attendu"""
    data = {
        'statement_type': ['IncomeStatement'],
        'sub_type1': ['Gross Profit'],
        'sub_type2': ['Revenue'],
        'sub_type3': ['Business Revenue'],
        'sub_type4': ['Business Revenue'],
        'sub_type5': ['Business Revenue'],
        'Q2 2021': [11958000000.0],
        'Q3 2021': [13757000000.0],
        'Q4 2021': [17719000000.0],
        'Q1 2022': [18756000000.0],
        'Q2 2022': [16934000000.0],
        'Q3 2022': [21454000000.0],
        'Q4 2022': [24318000000.0],
        'Q1 2023': [23329000000.0],
        'Q2 2023': [24927000000.0],
        'Q3 2023': [23350000000.0],
        'Q4 2023': [25167000000.0],
        'Q1 2024': [21301000000.0],
        'Q2 2024': [25500000000.0],
        'Q3 2024': [25182000000.0],
        'Q4 2024': [25707000000.0],
        'Q1 2025': [19335000000.0],
        'Q2 2025': [22496000000.0],
        'Q3 2025': [28095000000.0],
        'TTM': [95633000000.0]
    }
    return pd.DataFrame(data)


@pytest.mark.asyncio
@patch('morningpy.core.security_convert.IdSecurityConverter')
@patch('morningpy.core.client.BaseClient')
async def test_financial_statement_extractor(mock_base_client, mock_id_converter, fake_response):
    """Test de l'extracteur avec mock de la réponse API"""
    
    # Mock du converter
    mock_id_converter.return_value.convert.return_value = ["0P000115U4"]
    
    # Mock du client
    mock_client_instance = Mock()
    mock_client_instance.fetch = AsyncMock(return_value=fake_response)
    mock_base_client.return_value = mock_client_instance
    
    # Créer l'extracteur
    extractor = FinancialStatementExtractor(
        security_id=["0P000115U4"],
        statement_type="Income Statement",
        report_frequency="Quarterly"
    )
    
    # Exécuter
    result = await extractor.run()
    
    # Assertions
    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert 'statement_type' in result.columns
    assert result['statement_type'].iloc[0] == 'IncomeStatement'
    assert 'Q2 2021' in result.columns


def test_get_financial_statement_sync(fake_response, expected_dataframe):
    """Test de la fonction synchrone get_financial_statement"""
    
    with patch('morningpy.core.security_convert.IdSecurityConverter') as mock_id_converter, \
         patch('morningpy.core.client.BaseClient') as mock_base_client:
        
        # Configuration des mocks
        mock_id_converter.return_value.convert.return_value = ["0P000115U4"]
        
        mock_client_instance = Mock()
        mock_client_instance.fetch = AsyncMock(return_value=fake_response)
        mock_base_client.return_value = mock_client_instance
        
        # Appel de la fonction
        result = get_financial_statement(
            statement_type="Income Statement",
            report_frequency="Quarterly",
            security_id=["0P000115U4"]
        )
        
        # Assertions
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert list(result.columns) == list(expected_dataframe.columns)
        assert len(result) > 0


def test_process_response_with_real_data(fake_response):
    """Test unitaire de la méthode _process_response"""
    
    with patch('morningpy.core.security_convert.IdSecurityConverter') as mock_id_converter, \
         patch('morningpy.core.client.BaseClient') as mock_base_client:
        
        mock_id_converter.return_value.convert.return_value = ["0P000115U4"]
        mock_base_client.return_value = Mock()
        
        extractor = FinancialStatementExtractor(
            security_id=["0P000115U4"],
            statement_type="Income Statement",
            report_frequency="Quarterly"
        )
        
        # Test direct de _process_response
        result = extractor._process_response(fake_response)
        
        # Assertions
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert 'statement_type' in result.columns
        
        # Vérifier que les valeurs sont multipliées par 10^6
        if len(result) > 0 and 'Q2 2021' in result.columns:
            assert result['Q2 2021'].iloc[0] == 11958000000.0


def test_process_response_empty():
    """Test avec une réponse vide"""
    
    with patch('morningpy.core.security_convert.IdSecurityConverter') as mock_id_converter, \
         patch('morningpy.core.client.BaseClient') as mock_base_client:
        
        mock_id_converter.return_value.convert.return_value = ["0P000115U4"]
        mock_base_client.return_value = Mock()
        
        extractor = FinancialStatementExtractor(
            security_id=["0P000115U4"],
            statement_type="Income Statement",
            report_frequency="Quarterly"
        )
        
        # Test avec dict vide
        result = extractor._process_response({})
        assert isinstance(result, pd.DataFrame)
        assert result.empty
        
        # Test avec None
        result = extractor._process_response(None)
        assert isinstance(result, pd.DataFrame)
        assert result.empty