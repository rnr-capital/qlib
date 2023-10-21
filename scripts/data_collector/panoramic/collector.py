# Copyright (c) Panoramic Hills Capital.
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List, Optional


import fire
import pandas as pd
from abc import ABC, abstractmethod
from enum import Enum
from qlib.data.cache import H
from qlib.log import TimeInspector
from typing import Union, Callable
from panoramic.common.db.postgres import COMPUSTAT_DB as COMPUSTAT
from panoramic.common.db.postgres import ManagedSession, engine
from panoramic.common.model.compustat import IdxIndex, IdxDaily, IdxcstHi

CUR_DIR = Path(__file__).resolve().parent
sys.path.append(str(CUR_DIR.parent.parent))

from data_collector.index import IndexBase


class SecFilterBase(ABC):
    """ Class for filtering securities
    """
    @abstractmethod
    def valid_time(self, gvkey_s: Union[str, List[str]]) -> List[pd.Timestamp]:
        """ return the valid time of securities identified by gvkey_s
        """
        pass

    def _and(self, filter: SecFilterBase) -> SecFilterBase:
        """ combine two filters by "and"
        """
        class AndSecFilter(SecFilterBase):
            def valid_time(self, gvkey_s: Union[str, List[str]]) -> List[pd.Timestamp]:
                t1 = self.valid_time(gvkey_s)
                t2 = filter.valid_time(gvkey_s)
                return sorted(list(set(t1) & set(t2)))
            
            def __str__(self) -> str:
                return f"AndFilter<{self}, {filter}>"

        return AndSecFilter()

    def _or(self, filter: SecFilterBase) -> SecFilterBase:
        """ combine two filters by "or"
        """
        class OrSecFilter(SecFilterBase):
            def valid_time(self, gvkey_s: Union[str, List[str]]) -> List[pd.Timestamp]:
                t1 = self.valid_time(gvkey_s)
                t2 = filter.valid_time(gvkey_s)
                return sorted(list(set(t1) | set(t2)))
            
            def __str__(self) -> str:
                return f"OrFilter<{self}, {filter}>"

        return OrSecFilter()



class SecFilter(SecFilterBase):
    """ Filter security by static attribute, where an attribute is "{tablename}.{column}"
    """
    def __init__(self, att, eq=None, lt=None, leq=None, ht=None, heq=None) -> None:
        super().__init__()
        self.att = att
        self.eq = eq
        self.lt = lt
        self.leq = leq
        self.ht = ht
        self.heq = heq
        atts = att.split(".")
        if len(atts) != 2:
            raise ValueError(f"Invalid attribute {att}")
        self.tablename = atts[0]
        self.column = atts[1]








class PanoIndex(IndexBase):
    """ Create index by filtering on security attributes.
        Example Usage:
        ```
        from qlib.scripts.data_collector.panoramic.collector import F

        idx = PanoIndex(
            "pano_idx",
            F("gvkey").eq("001004"),
            qlib_dir="/tmp/qlib_data/pano_idx",
            freq="day",
        )

        idx = PanoIndex(
            "pano_idx",
            F("gvkey").eq("001004") & F("iid").eq("01"),
            qlib_dir="/tmp/qlib_data/pano_idx",
            freq="day",
        )
        ```
    
    """
    def __init__(
        self,
        index_name: str,
        filter: SecFilterBase,
        qlib_dir: Optional[str | Path] = None,
        freq: str = "day",
        request_retry: int = 5,
        retry_sleep: int = 3,
    ):
        super().__init__(index_name, qlib_dir, freq, request_retry, retry_sleep)
