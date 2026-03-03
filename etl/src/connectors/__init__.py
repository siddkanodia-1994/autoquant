from .base import BaseConnector, ExtractionResult, ConnectorSource, ExtractionStatus

# Lazy imports to avoid requiring playwright/httpx at module load time.
# Import specific connectors directly when needed:
#   from src.connectors.vahan import VahanConnector
#   from src.connectors.fada import FADAConnector
#   from src.connectors.bse_wholesale import BSEWholesaleConnector
#   from src.connectors.siam_historical import SIAMHistoricalConnector

__all__ = [
    "BaseConnector",
    "ExtractionResult",
    "ConnectorSource",
    "ExtractionStatus",
]
