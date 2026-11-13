from pydantic import BaseModel


class ApplyRequest(BaseModel):
    stream_id: int


class StreamPatch(BaseModel):
    enabled: bool | None = None


class SettingPut(BaseModel):
    value: str


class RebuildStartRequest(BaseModel):
    stream_id: int


class RebuildConfirmRequest(BaseModel):
    pass


class RebuildRejectRequest(BaseModel):
    reason: str = ""


class RebuildTamperRequest(BaseModel):
    stream_id: int
    bad_cursor: int
