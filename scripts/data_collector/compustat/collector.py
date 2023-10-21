# Copyright (c) Panoramic Hills Capital.

import sys
from pathlib import Path
from typing import Any, List, Optional

import fire
import pandas as pd
from qlib.data.cache import H
from qlib.log import TimeInspector
from panoramic.common.db.postgres import COMPUSTAT_DB as COMPUSTAT
from panoramic.common.db.postgres import ManagedSession, engine
from panoramic.common.model.compustat import IdxIndex, IdxDaily, IdxcstHi

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from data_collector.index import IndexBase

IdxcstHiColumns = ['gvkey', 'iid', 'gvkeyx', '_from', 'thru']

class CompustatIndex(IndexBase):
    TABLE_NAME = "idx_index"

    def __init__(
        self,
        gvkeyx: str,
        qlib_dir: Optional[str | Path] = None,
        freq: str = "day",
        request_retry: int = 5,
        retry_sleep: int = 3,
    ):
        self.db_obj = self._db_init(gvkeyx)
        super().__init__(self.db_obj.conm, qlib_dir, freq, request_retry, retry_sleep)
        self.gvkeyx = gvkeyx

    def _db_init(self, gvkeyx):
        flag = f"{self.TABLE_NAME}_{gvkeyx}_idx_index"
        if flag not in H["x"]:
            with ManagedSession(db=COMPUSTAT) as session, TimeInspector.logt("Pulling IdxIndex from DB"):
                # search IdxIndex by similar index_name
                obj = session.query(IdxIndex).filter(IdxIndex.gvkeyx == gvkeyx).first()  # type: ignore
                if not obj:
                    raise ValueError(f"Index {gvkeyx} not found in {self.TABLE_NAME}")
                session.expunge_all()
                H["x"][flag] = obj
        return H["x"][flag]

    def __getattr__(self, __name: str) -> Any:
        if hasattr(self.db_obj, __name):
            return getattr(self.db_obj, __name)
        else:
            raise AttributeError(f"{self.__class__.__name__} has no attribute {__name}")

    @property
    def bench_start_date(self) -> pd.Timestamp:
        flag = f"{IdxDaily.__tablename__}_{self.gvkeyx}_first_row"
        if flag not in H["x"]:
            with ManagedSession(db=COMPUSTAT) as session, TimeInspector.logt("Pulling bench_start_date from DB"):
                # search IdxDaily by gvkeyx and get the first row by datadate
                obj = session.query(IdxDaily).filter(IdxDaily.gvkeyx == self.gvkeyx).order_by(IdxDaily.datadate).first() # type: ignore
                if not obj:
                    raise ValueError(f"Index {self.gvkeyx} not found in {IdxDaily.__tablename__}")
                session.expunge_all()
                H["x"][flag] = obj
        return H["x"][flag].datadate
            
    @property
    def calendar_list(self) -> List[pd.Timestamp]:
        flag = f"{self.TABLE_NAME}_{self.gvkeyx}_calendar_list"
        if flag not in H["x"]:
            with ManagedSession(db=COMPUSTAT) as session, TimeInspector.logt("Pulling calendar_list from DB"):
                # search IdxDaily by gvkeyx and get the all the datadate column (only the datadate column)
                objs = session.query(IdxDaily.datadate).filter(IdxDaily.gvkeyx == self.gvkeyx).all() # type: ignore
                if not objs:
                    raise ValueError(f"Index {self.gvkeyx} not found in {IdxDaily.__tablename__}")
                session.expunge_all()
                H["x"][flag] = [obj.datadate for obj in objs]
        return H["x"][flag]

    def get_new_companies(self) -> pd.DataFrame:
        flag = f"{self.TABLE_NAME}_{self.gvkeyx}_new_companies"
        if flag not in H["x"]:
            with ManagedSession(db=COMPUSTAT) as session, TimeInspector.logt("Pulling get_new_companies from DB"):
                # search idxcst_his by gvkeyx and get the all the rows, and convert all the rows into pandas dataframe
                objs = session.query(IdxcstHi).filter(IdxcstHi.gvkeyx == self.gvkeyx).all() # type: ignore
                if not objs:
                    raise ValueError(f"Index {self.gvkeyx} not found in {IdxcstHi.__tablename__}")
                session.expunge_all()
                H["x"][flag] = pd.DataFrame([{key: obj.__dict__[key]} for obj in objs for key in IdxcstHiColumns])
        return H["x"][flag]
