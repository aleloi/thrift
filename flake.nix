{
  description = "Zig arrow and nanoarrow";

  inputs = {
    nixpkgs.url          = "github:NixOS/nixpkgs/nixos-25.05";
    zig-overlay = {
      url = "github:mitchellh/zig-overlay";
    };
  };

  outputs = {self, zig-overlay, nixpkgs, ... }:
    let
      system = "x86_64-linux";
      myOverlays = [
        zig-overlay.overlays.default
      ];
      pkgs = import nixpkgs {
        inherit system;
        overlays = myOverlays;
      };
    in
      {
        devShell.x86_64-linux = pkgs.mkShell {
          nativeBuildInputs = [
            zig-overlay.packages.${pkgs.system}."master-2025-08-03"
            pkgs.pkg-config
            pkgs.python313
          ];
        };
      };
}
            
