{ pkgs ? import <nixpkgs> {} }:

(import ./build.nix) {
  pkgs = pkgs;
}
