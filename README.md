# Hyrise Example Plugin

This repository contains an example plugin for Hyrise. It can be used as a basis for the development of further plugins.

## Development

1. Take a look at the example_plugin files. These can be used as a blueprint for other plugins.
1. Implement your plugin according to the example plugin. Make sure it inherits from `AbstractPlugin` and `Singleton` (hyrise/src/lib/utils/(abstract_plugin || singleton).hpp).
1. Add the necessary information to build your plugin to CMakeLists.txt. A self-explanatory skeleton is provided in the file itself.
1. We highly recommend out-of-source builds, e.g.:
    - `mkdir cmake-build-debug && cd cmake-build-debug`
    - `cmake ..`
    - `make ExamplePlugin -j 12`
1. Test your plugin, for example, using the Hyrise Console (from the main directory):
    - `./cmake-build-debug/hyriseConsole`
    - `load_plugin ExamplePlugin cmake-build-debug/lib/libExamplePlugin.dylib`


- formatting and linting
- build process CI