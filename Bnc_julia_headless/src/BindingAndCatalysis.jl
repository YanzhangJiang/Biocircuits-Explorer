ENV["BNC_HEADLESS"] = "1"
Base.include(Base.__toplevel__, joinpath(@__DIR__, "..", "..", "Bnc_julia", "src", "BindingAndCatalysis.jl"))
