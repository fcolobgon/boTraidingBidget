##-*- coding: utf-8 -*-
from typing import List
from xmlrpc.client import Boolean

from src.botrading.utils.bitget_data_util import DataFrameColum
from src.botrading.utils.bitget_data_util import ColumStateValues


class NotSupportedException(Exception):
    def __init__(self, message, errors):
        super(NotSupportedException, self).__init__(message)
        self.errors = errors


class RuleUtils:
    
    @staticmethod
    def is_valid_state_buy(nameState):
        return (
            nameState == ColumStateValues.WAIT.value
            or nameState == ColumStateValues.SELL.value
            or nameState == ColumStateValues.BLCK_INS_MNY.value
        )

    @staticmethod
    def get_rules_search_by_state_wait_or_sell():
        states = [ColumStateValues.WAIT.value, ColumStateValues.SELL.value]
        RuleUtils.get_rules_search_by_states(states)

    @staticmethod
    def get_rules_search_by_states(states: List[ColumStateValues]):
        state_name = DataFrameColum.STATE.value + " == "
        rule = "("
        for i, state in enumerate(states):
            rule = rule + state_name + '"' + state.value + '"'
            if i != len(states) - 1:
                rule = rule + " or "
            else:
                rule = rule + ")"

        return rule


