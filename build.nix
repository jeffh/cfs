{ pkgs ? import <nixpkgs> {}, overrides ? {} }:
with pkgs;

let
  defaults = {
    pname = "cfs";
    version = "git";
    goPackagePath = "github.com/jeffh/cfs";

    buildInputs = stdenv.lib.optionals stdenv.isDarwin [ darwin.apple_sdk.frameworks.Security ];

    src = nix-gitignore.gitignoreSource [] ./.;
    modSha256 = "04k6wlwwdz583760w1z6cwy2g7hdsi2dhl1vqqfjcz3w0hdnvx1y";

    meta = with stdenv.lib; {
      description = "Cloud File System. Provides Plan9 File System interfaces to various services.";
      homepage = https://github.com/jeffh/cfs;
      maintainers = with maintainers; [ jeffh ];
      platforms = platforms.darwin ++ platforms.linux;
    };
  };
in
  assert lib.versionAtLeast go.version "1.14";
  buildGoModule (defaults // overrides)
