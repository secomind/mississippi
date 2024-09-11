# Copyright 2024 SECO Mind Srl
# SPDX-License-Identifier: Apache-2.0

# Used by "mix format"
[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  import_deps: [:typed_struct],
  plugins: [Styler]
]
