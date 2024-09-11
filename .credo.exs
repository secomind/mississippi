# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

%{
  configs: [
    %{
      name: "default",
      files: %{
        included: ["lib/", "test/"]
      },
      strict: true,
      color: true,
      checks: %{
        extra: [{Credo.Check.Readability.MaxLineLength, max_length: 122}],
        disabled: [
          # this means that `TagTODO` will not run
          {Credo.Check.Design.TagTODO, []}
        ]
      }
    }
  ]
}
