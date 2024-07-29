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
        disabled: [
          # this means that `TagTODO` will not run
          {Credo.Check.Design.TagTODO, []}
        ]
      }
    }
  ]
}
