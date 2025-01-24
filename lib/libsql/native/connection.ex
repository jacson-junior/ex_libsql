defmodule LibSQL.Native.Connection do
  @type t :: %__MODULE__{
          conn_ref: reference()
        }
  defstruct [:conn_ref]
end
