import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional

from app.config import settings
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class SheetLogic:
    number: int
    title: str
    code: str
    classification: List[str] = field(default_factory=list)
    required_inputs: List[str] = field(default_factory=list)
    output_columns: List[str] = field(default_factory=list)
    processing_logic: List[str] = field(default_factory=list)

    @property
    def key(self) -> str:
        return self.code


class GSTR1RuleBook:
    SECTION_PATTERN = re.compile(r"^##\s+SHEET\s+(\d+):\s+(.+)$")
    SUBSECTION_PATTERN = re.compile(r"^\*\*(.+?):\*\*$")

    def __init__(self, rules_path: Optional[str] = None):
        self.rules_path = Path(rules_path or settings.GSTR1_RULES_PATH)
        self._rules: Dict[str, SheetLogic] = {}
        self._load_rules()

    @staticmethod
    def _slugify(title: str) -> str:
        primary = title.split(",")[0].strip()
        primary = primary.split(" ")[0].strip()
        return re.sub(r"[^a-z0-9]+", "", primary.lower())

    def _load_rules(self) -> None:
        try:
            text = self.rules_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            logger.warning("GSTR-1 rules file not found at %s", self.rules_path)
            return

        current_logic: Optional[SheetLogic] = None
        current_section: Optional[str] = None

        for raw_line in text.splitlines():
            line = raw_line.rstrip()
            if not line:
                continue

            section_match = self.SECTION_PATTERN.match(line)
            if section_match:
                if current_logic:
                    self._rules[current_logic.key] = current_logic
                number = int(section_match.group(1))
                title = section_match.group(2).strip()
                code = self._slugify(title)
                current_logic = SheetLogic(number=number, title=title, code=code)
                current_section = None
                continue

            subsection_match = self.SUBSECTION_PATTERN.match(line)
            if subsection_match and current_logic:
                current_section = subsection_match.group(1).strip().lower()
                continue

            if line.startswith("- ") and current_logic and current_section:
                content = line[2:].strip()
                if current_section.startswith("classification"):
                    current_logic.classification.append(content)
                elif current_section.startswith("required input"):
                    current_logic.required_inputs.append(content)
                elif current_section.startswith("output column"):
                    current_logic.output_columns.append(content)
                elif current_section.startswith("processing logic"):
                    current_logic.processing_logic.append(content)

        if current_logic:
            self._rules[current_logic.key] = current_logic

        logger.info(
            "Loaded %s GSTR-1 rule sections from %s",
            len(self._rules),
            self.rules_path,
        )

    def get(self, code: str) -> Optional[SheetLogic]:
        return self._rules.get(code.lower())

    def all(self) -> List[SheetLogic]:
        return list(self._rules.values())


@lru_cache()
def get_rule_book() -> GSTR1RuleBook:
    return GSTR1RuleBook()













