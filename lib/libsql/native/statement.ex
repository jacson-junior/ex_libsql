defmodule LibSQL.Native.Statement do
  @type t :: %__MODULE__{
          stmt_ref: reference()
        }
  defstruct stmt_ref: nil
end
