# conftest.py - Configuration globale des tests
import pytest
from unittest.mock import Mock, AsyncMock, patch
import pandas as pd
from pathlib import Path
import json


@pytest.fixture
def mock_base_client():
    """Mock du client de base réutilisable"""
    with patch('your_module.BaseClient') as mock:
        client_instance = Mock()
        client_instance.fetch = AsyncMock()
        mock.return_value = client_instance
        yield client_instance


@pytest.fixture
def mock_id_converter():
    """Mock du convertisseur d'ID réutilisable"""
    with patch('your_module.IdSecurityConverter') as mock:
        converter_instance = Mock()
        converter_instance.convert.return_value = ["0P000115U4"]
        mock.return_value = converter_instance
        yield converter_instance


# test_financial_statement.py
import pytest
import pandas as pd
from unittest.mock import AsyncMock
import json
from pathlib import Path


class TestFinancialStatementExtractor:
    """Tests pour FinancialStatementExtractor avec approche orientée données"""
    
    @pytest.fixture
    def api_test_data(self):
        """
        Fixture centrale contenant toutes les données de test.
        Structure extensible pour toutes vos APIs.
        """
        return {
            "income_statement_quarterly": {
                "request": {
                    "security_id": ["0P000115U4"],
                    "statement_type": "Income Statement",
                    "report_frequency": "Quarterly"
                },
                "mock_response": {
                    "columnDefs": [
                        "Q1 2020", "Q2 2021", "Q3 2021", "Q4 2021", "TTM"
                    ],
                    "rows": [
                        {
                            "label": "IncomeStatement",
                            "subLevel": [
                                {
                                    "label": "Gross Profit",
                                    "datum": ["_PO_", 2884.0, 3660.0, 4847.0, 16264.0],
                                    "subLevel": [
                                        {
                                            "label": "Total Revenue",
                                            "datum": ["_PO_", 11958.0, 13757.0, 17719.0, 95633.0],
                                            "subLevel": [
                                                {
                                                    "label": "Business Revenue",
                                                    "datum": ["_PO_", 11958.0, 13757.0, 17719.0, 95633.0]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ],
                    "footer": {"currency": "USD"}
                },
                "expected": {
                    "columns": ["statement_type", "sub_type1", "sub_type2", "sub_type3", 
                               "sub_type4", "sub_type5", "Q2 2021", "Q3 2021", "Q4 2021", "TTM"],
                    "row_count": 1,
                    "statement_type_value": "IncomeStatement",
                    "first_value_q2_2021": 11958000000.0,  # vérifie la multiplication par 10^6
                    "sub_type1_value": "Gross Profit"
                }
            },
            "balance_sheet_annual": {
                "request": {
                    "security_id": ["0P000115U4"],
                    "statement_type": "Balance Sheet",
                    "report_frequency": "Annualy"
                },
                "mock_response": {
                    # ... structure similaire pour balance sheet
                },
                "expected": {
                    # ... attentes pour balance sheet
                }
            }
        }
    
    @pytest.mark.parametrize("test_case", [
        "income_statement_quarterly",
        # "balance_sheet_annual",  # Ajoutez vos autres cas ici
    ])
    def test_financial_statement_success(
        self, 
        test_case, 
        api_test_data, 
        mock_base_client, 
        mock_id_converter
    ):
        """Test paramétré pour tous les types de statements"""
        
        # Récupérer les données du test
        test_data = api_test_data[test_case]
        
        # Configurer le mock avec la réponse
        mock_base_client.fetch.return_value = test_data["mock_response"]
        
        # Exécuter
        from your_module import get_financial_statement
        result = get_financial_statement(**test_data["request"])
        
        # Assertions génériques
        assert isinstance(result, pd.DataFrame), "Result should be a DataFrame"
        assert not result.empty, "DataFrame should not be empty"
        
        # Assertions spécifiques basées sur les attentes
        expected = test_data["expected"]
        
        # Vérifier les colonnes
        assert list(result.columns) == expected["columns"], \
            f"Columns mismatch. Expected: {expected['columns']}, Got: {list(result.columns)}"
        
        # Vérifier le nombre de lignes
        assert len(result) == expected["row_count"], \
            f"Expected {expected['row_count']} rows, got {len(result)}"
        
        # Vérifier les valeurs spécifiques
        if "statement_type_value" in expected:
            assert result['statement_type'].iloc[0] == expected["statement_type_value"]
        
        if "first_value_q2_2021" in expected and "Q2 2021" in result.columns:
            assert result['Q2 2021'].iloc[0] == expected["first_value_q2_2021"], \
                "Values should be multiplied by 10^6"
    
    def test_empty_response(self, mock_base_client, mock_id_converter):
        """Test avec réponse vide"""
        mock_base_client.fetch.return_value = {}
        
        from your_module import get_financial_statement
        result = get_financial_statement(
            security_id=["0P000115U4"],
            statement_type="Income Statement",
            report_frequency="Quarterly"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_none_response(self, mock_base_client, mock_id_converter):
        """Test avec None response"""
        mock_base_client.fetch.return_value = None
        
        from your_module import get_financial_statement
        result = get_financial_statement(
            security_id=["0P000115U4"],
            statement_type="Income Statement",
            report_frequency="Quarterly"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert result.empty
    
    def test_api_error(self, mock_base_client, mock_id_converter):
        """Test quand l'API retourne une erreur"""
        mock_base_client.fetch.side_effect = Exception("API Error")
        
        from your_module import get_financial_statement
        
        with pytest.raises(Exception) as exc_info:
            get_financial_statement(
                security_id=["0P000115U4"],
                statement_type="Income Statement",
                report_frequency="Quarterly"
            )
        
        assert "API Error" in str(exc_info.value)
    
    def test_malformed_response(self, mock_base_client, mock_id_converter):
        """Test avec réponse malformée"""
        mock_base_client.fetch.return_value = {
            "columnDefs": ["Q1 2021"],
            "rows": "invalid_structure"  # Should be a list
        }
        
        from your_module import get_financial_statement
        result = get_financial_statement(
            security_id=["0P000115U4"],
            statement_type="Income Statement",
            report_frequency="Quarterly"
        )
        
        # Le comportement dépend de votre implémentation
        # Soit ça lève une exception, soit ça retourne un DataFrame vide
        assert isinstance(result, pd.DataFrame)
    
    @pytest.mark.parametrize("invalid_input,error_type", [
        ({"statement_type": None}, ValueError),
        ({"report_frequency": "Invalid"}, ValueError),
        ({"security_id": []}, ValueError),
    ])
    def test_invalid_inputs(
        self, 
        invalid_input, 
        error_type, 
        mock_base_client, 
        mock_id_converter
    ):
        """Test avec des inputs invalides"""
        from your_module import get_financial_statement
        
        base_params = {
            "security_id": ["0P000115U4"],
            "statement_type": "Income Statement",
            "report_frequency": "Quarterly"
        }
        base_params.update(invalid_input)
        
        with pytest.raises(error_type):
            get_financial_statement(**base_params)


# test_process_response.py - Tests unitaires de la logique métier
class TestProcessResponse:
    """Tests unitaires de _process_response isolés de l'API"""
    
    @pytest.fixture
    def extractor_instance(self, mock_base_client, mock_id_converter):
        """Instance de l'extracteur pour tester les méthodes internes"""
        from your_module import FinancialStatementExtractor
        
        return FinancialStatementExtractor(
            security_id=["0P000115U4"],
            statement_type="Income Statement",
            report_frequency="Quarterly"
        )
    
    def test_value_multiplication(self, extractor_instance):
        """Test que les valeurs sont bien multipliées par 10^6"""
        response = {
            "columnDefs": ["Q1", "Q2"],
            "rows": [{
                "label": "IncomeStatement",
                "subLevel": [{
                    "label": "Revenue",
                    "datum": [100.0, 200.0]
                }]
            }]
        }
        
        result = extractor_instance._process_response(response)
        
        assert result['Q1'].iloc[0] == 100000000.0
        assert result['Q2'].iloc[0] == 200000000.0
    
    def test_null_handling(self, extractor_instance):
        """Test de la gestion des valeurs nulles"""
        response = {
            "columnDefs": ["Q1", "Q2"],
            "rows": [{
                "label": "IncomeStatement",
                "subLevel": [{
                    "label": "Revenue",
                    "datum": [None, "_PO_", 100.0]
                }]
            }]
        }
        
        result = extractor_instance._process_response(response)
        
        # Vérifier que None et "_PO_" sont convertis en 0
        assert result['Q1'].iloc[0] == 0.0
    
    def test_nested_structure_flattening(self, extractor_instance):
        """Test de l'aplatissement de la structure nested"""
        response = {
            "columnDefs": ["Q1"],
            "rows": [{
                "label": "IncomeStatement",
                "subLevel": [{
                    "label": "Level1",
                    "subLevel": [{
                        "label": "Level2",
                        "subLevel": [{
                            "label": "Level3",
                            "datum": [100.0]
                        }]
                    }]
                }]
            }]
        }
        
        result = extractor_instance._process_response(response)
        
        # Vérifier la profondeur des colonnes sub_type
        assert 'sub_type1' in result.columns
        assert 'sub_type2' in result.columns
        assert 'sub_type3' in result.columns


# test_integration.py - Tests d'intégration
class TestIntegration:
    """Tests d'intégration avec vraies données (optionnel, à skip en CI)"""
    
    @pytest.mark.integration
    @pytest.mark.skip(reason="Requires real API access")
    def test_real_api_call(self):
        """Test avec un vrai appel API"""
        from your_module import get_financial_statement
        
        result = get_financial_statement(
            security_id=["0P000115U4"],
            statement_type="Income Statement",
            report_frequency="Quarterly"
        )
        
        assert isinstance(result, pd.DataFrame)
        assert not result.empty
        assert 'statement_type' in result.columns


# test_data_validation.py - Tests de validation des données
class TestDataValidation:
    """Tests pour valider la cohérence des données"""
    
    def test_data_consistency(self, mock_base_client, mock_id_converter):
        """Test de cohérence des données financières"""
        mock_base_client.fetch.return_value = {
            "columnDefs": ["Q1 2021", "Q2 2021"],
            "rows": [{
                "label": "IncomeStatement",
                "subLevel": [
                    {
                        "label": "Revenue",
                        "datum": [1000.0, 1500.0]
                    },
                    {
                        "label": "Cost",
                        "datum": [-800.0, -1200.0]
                    }
                ]
            }]
        }
        
        from your_module import get_financial_statement
        result = get_financial_statement(
            security_id=["0P000115U4"],
            statement_type="Income Statement",
            report_frequency="Quarterly"
        )
        
        # Vérifier que les coûts sont négatifs
        cost_rows = result[result['sub_type1'] == 'Cost']
        assert all(cost_rows['Q1 2021'] < 0)
        
        # Vérifier que les revenus sont positifs
        revenue_rows = result[result['sub_type1'] == 'Revenue']
        assert all(revenue_rows['Q1 2021'] > 0)
```

**Structure de fichiers optimale :**
```
tests/
├── conftest.py                      # Fixtures globales
├── unit/
│   ├── test_process_response.py     # Tests unitaires de la logique
│   └── test_data_transformation.py
├── integration/
│   ├── test_financial_statement.py  # Tests d'intégration
│   ├── test_balance_sheet.py
│   └── test_cash_flow.py
├── fixtures/
│   ├── income_statement_response.json
│   ├── balance_sheet_response.json
│   └── expected_outputs.json
└── helpers/
    └── test_utils.py                # Utilitaires de test