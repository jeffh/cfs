{ pkgs ? import <nixpkgs> {}, fetchGit ? builtins.fetchGit }:

(import ./build.nix) {
  pkgs = pkgs;
  overrides = {
    version = "0.1.0";
    src = fetchGit {
      url = "ssh://git@github.com/jeffh/cfs.git";
      rev = "e5d31a029ee1b32515099ade98967bfcd7840cc5";
    };
  };
}
