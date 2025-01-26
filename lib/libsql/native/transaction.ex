defmodule ExLibSQL.Native.Transaction do
  @type t :: %__MODULE__{
          tx_ref: reference()
        }
  defstruct tx_ref: nil
end
