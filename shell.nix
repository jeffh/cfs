let 
  pkgs = import <nixpkgs> {};
in
  with pkgs;

  mkShell {
    buildInputs = [
      go
    ] ++ stdenv.lib.optionals stdenv.isDarwin [
      darwin.apple_sdk.frameworks.Security
    ];

    shellHook = ''
      unset GOPATH
      '';
  }
