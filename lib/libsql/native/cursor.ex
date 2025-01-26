defmodule ExLibSQL.Native.Cursor do
  @type t :: %__MODULE__{
          cur_ref: reference()
        }
  defstruct [:cur_ref]
end
