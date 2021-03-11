from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, contains_eager

from datetime import date, timedelta
import sys

class CMGRerunError:
    engine_activity = None

    def __init__(self, **kwargs):
        self.engine_activity = create_engine('mysql+pymysql://fmadmin:Fm%40min2019@10.101.5.49/ActivityThDb')

    def rerun(self):
        try:
            stock_sql_query = 'update ActivityThDb.ProcessActivity pa set pa.ProcessActivityStatus = \'ReadyToStart\' where pa.ProcessActivityStatus = \'ErrorOnStart\' and \
                            pa.ActivityCode in (\'cmgLazadaOrder\', \'cmgShopeeOrder\', \'cmgSendReserve\', \'cmgSendUnreserve\', \'cmgSendUnreserveCancel\', \
                                                \'cfrRTSReserveHd\', \
                                                \'cfrRTSReserveCnc\', \
                                                \'cfrPiByorderStdHd\', \
                                                \'cfrPiByorderExpressHd\', \
                                                \'cfrPiByorderStdCnc\', \
                                                \'cfrPaByorderStdHd\', \
                                                \'cfrPaByorderExpressHd\', \
                                                \'cfrPaByorderStdCnc\', \
                                                \'CdsSecretCode\', \
                                                \'rbsSecretCode\', \
                                                \'rbsSecretCode\', \
                                                \'CdsUpdateCto\', \
                                                \'CdsUpdateCtoByOrder\', \
                                                \'CdsReadytoCollect\', \
                                                \'CdsCncPickUp\', \
                                                \'CdsCncRequest\', \
                                                \'CdsPaOrderHdHigh\', \
                                                \'CdsPaOrderHdNorm\', \
                                                \'CdsPaWaveBDC\', \
                                                \'CdsPaWaveInstore\', \
                                                \'CdsPaWaveHdHigh\', \
                                                \'CdsPaOrderBDC\', \
                                                \'CdsPaOrderInstore\', \
                                                \'CdsPaOrderHdLow\', \
                                                \'CdsPaWaveHdNorm\', \
                                                \'CdsPaWaveHdLow\', \
                                                \'CdsPaOrderHighInstore\', \
                                                \'CdsPaOrderHighBDC\', \
                                                \'CdsPaOrderMedBDC\', \
                                                \'CdsPaOrderLowBDC\', \
                                                \'CdsPaOrderMedInstore\', \
                                                \'CdsPaOrderLowInstore\', \
                                                \'CdsPaOrderLowHdFlash\', \
                                                \'CdsPaWaveLowHdFlash\', \
                                                \'cdsPaOrderHdCentralExp\', \
                                                \'CdsPiOrderNoneHigh\', \
                                                \'CdsPiOrderNoneNorm\', \
                                                \'CdsPiWaveCat12BrandRobinN\', \
                                                \'CdsPiOrderCat12BrandlessN\', \
                                                \'CdsPiWaveCat2High\', \
                                                \'CdsPiOrderNoneLow\', \
                                                \'CdsPiOrderCat12BrandHigh\', \
                                                \'CdsPiOrderCat12BrandMed\', \
                                                \'CdsPiOrderCat12BrandLow\', \
                                                \'CdsPaCancelled\', \
                                                \'CdsPiCancelled\', \
                                                \'CdsReserve\', \
                                                \'CdsUnreserve\', \
                                                \'subOrderUpdated2000\', \
                                                \'subOrderUpdated2001\', \
                                                \'subOrderUpdated2002\', \
                                                \'subOrderUpdated2003\', \
                                                \'subOrderUpdated2004\', \
                                                \'subOrderUpdated2005\', \
                                                \'subOrderUpdated2006\', \
                                                \'subOrderUpdated2007\', \
                                                \'subOrderUpdated2008\', \
                                                \'subOrderUpdated3000byOrder\', \
                                                \'subOrderUpdated3000\', \
                                                \'subOrderUpdated4100\', \
                                                \'CdsUpdateCtoByTracking\', \
                                                \'CdsCcReturn\', \
                                                \'rbsReserve\', \
                                                \'rbsPiOrderCatBrandLow\', \
                                                \'rbsPiOrderNoneHigh\', \
                                                \'rbsPiOrderNoneMed\', \
                                                \'rbsPiWaveCatBrandRobinLow\', \
                                                \'rbsPiWaveCatLow\', \
                                                \'rbsPiOrderCatBrandMed\', \
                                                \'rbsPiOrderCatBrandHigh\', \
                                                \'rbsPiOrderNoneLow\', \
                                                \'rbsPaOrderHdHigh\', \
                                                \'rbsPaOrderHdMed\', \
                                                \'rbsPaWaveBDCHigh\', \
                                                \'rbsPaWaveInstoreLow\', \
                                                \'rbsPaWaveHdHigh\', \
                                                \'rbsPaOrderBDCHigh\', \
                                                \'rbsPaOrderBDCMed\', \
                                                \'rbsPaOrderBDCLow\', \
                                                \'rbsPaOrderInstoreHigh\', \
                                                \'rbsPaOrderInstoreMed\', \
                                                \'rbsPaOrderInstoreLow\', \
                                                \'rbsPaWaveHdLow\', \
                                                \'rbsPaWaveBDCMed\', \
                                                \'rbsPaWaveBDCLow\', \
                                                \'rbsPaWaveInstoreHigh\', \
                                                \'rbsPaWaveInstoreMed\', \
                                                \'rbsPaWaveHdMed\', \
                                                \'rbsPaCancelled\', \
                                                \'rbsPiCancelled\', \
                                                \'rbsCcReturn\', \
                                                \'rbsUpdateToCTO\', \
                                                \'rbsUpdateToCTOByOrder\', \
                                                \'rbsReadytoCollect\', \
                                                \'rbsUnreserve\', \
                                                \'rbsUpdateCtoByTracking\', \
                                                \'rbsCncPickUp\', \
                                                \'rbsCncRequest\', \
                                                \'cdsLineNotifyPicking\', \
                                                \'rbsLineNotifyPicking\' \
                            ) \
                            and pa.UpdatedDate > DATE_ADD(NOW() , INTERVAL -5 DAY) and pa.CreatedDate > DATE_ADD(NOW() , INTERVAL -3 DAY)'

            result = self.engine_activity.execute(stock_sql_query)
            print(result)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            raise

if __name__ == "__main__":
    kwargs = {}
    report = CMGRerunError(**kwargs)
    report.rerun()

