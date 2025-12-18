{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
  };

  outputs = { self, nixpkgs }:
    let
      lib = nixpkgs.lib;
      allSystems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-linux"
      ];
      pkgsFor = lib.genAttrs allSystems (system: import nixpkgs { inherit system; config.allowUnfree = true; });
      overEachSystem = f: lib.genAttrs allSystems (system: f { inherit system; pkgs = pkgsFor.${system}; });
    in {
      formatter = overEachSystem ({ system, pkgs }: pkgs.nixfmt-rcf-style);
      devShells = overEachSystem ({ system, pkgs }: {
        default = pkgs.mkShellNoCC {
          packages = [
            pkgs.clojure
            pkgs.babashka
          ];
        };
      });
    };
}
