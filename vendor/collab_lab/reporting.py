"""Editable report-template tool. Tracks provenance, never validates report fields."""
import copy
from pathlib import Path
from .common import ROOT, read_json, write_json, digest
from .environment import ContractError


class ReportTemplateTool:
    def __init__(self, directory, source=None):
        self.directory=Path(directory)
        path=Path(source) if source else ROOT/"templates"/"capability_gap_report.json"
        self.spec=read_json(path)
        self.sha256=digest(self.spec)
        self.fetched=False
        write_json(self.directory/"report_template_source.json",self.spec)

    def get(self):
        self.fetched=True
        result={"template_id":self.spec["id"],"template_version":self.spec["version"],
                "template_sha256":self.sha256,"purpose":self.spec["purpose"],
                "editing_policy":self.spec["editing_policy"],"editable_report":copy.deepcopy(self.spec["report"]),
                "field_guidance":copy.deepcopy(self.spec["field_guidance"]),
                "next":"自行填充或改写草稿，再通过report_gap提交。字段可增删改；不会自动合并回原始模板。"}
        write_json(self.directory/"report_template_returned.json",result)
        return result

    def require_fetched(self):
        if not self.fetched:
            raise ContractError("能力报告采用模板辅助填充。请先调用get_report_template获取可编辑草稿，再自行填充或改写并report_gap；字段可任意增删改。")

    def provenance(self):
        return {"mode":"template_assisted" if self.fetched else "direct_api_without_template",
                "template_id":self.spec["id"],"template_version":self.spec["version"],"template_sha256":self.sha256,
                "template_fetched":self.fetched,"fields_editable":True,"content_schema_enforced":False,
                "auto_merge_or_fill_performed":False}
