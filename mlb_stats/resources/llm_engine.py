from typing import List, Type

import langchain_core.language_models
from dagster import (ConfigurableResource)
from langchain.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel


class ColumnMapping(BaseModel):
    old_column: str
    new_column: str


class DataframeColumns(BaseModel):
    columns: Type[ColumnMapping]

    def to_dict(self):
        return {column.old_column: column.new_column for column in self.columns}


class ColumnRenamer(ConfigurableResource):
    llm_model: langchain_core.language_models.BaseChatModel

    def get_column_map(self, columns: List[str], convertions: List[str]) -> DataframeColumns:
        parser = PydanticOutputParser(pydantic_object=DataframeColumns)
        prompt_template: ChatPromptTemplate = ChatPromptTemplate([
            ('system',
             " You are a analyst working with a dataframe. Your task is to"
             " rename the columns using the following conventions: {conventions}. "
             "Provide a key-value mapping of the old column names to the new column names."),
            ('human', "The columns are: {columns}"),
        ]
        ).partial(format_instructions=parser.get_format_instructions())
        chain = prompt_template | self.llm_model | parser
        return chain.invoke(
            {"columns": ",".join(columns),
             "conventions": ",".join(convertions)}
        )
