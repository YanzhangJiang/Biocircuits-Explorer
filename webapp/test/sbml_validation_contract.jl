using Test
using HTTP
using JSON3

function _sbml_kd_validation_document(value::AbstractString;
        local_parameter::Bool=false, include_value::Bool=true)
    value_attribute = include_value ? " value=\"$value\"" : ""
    parameters = local_parameter ? "" : """
        <listOfParameters>
          <parameter id="Kd_bind"$value_attribute constant="true"/>
        </listOfParameters>
    """
    kinetic_law = if local_parameter
        """
            <kineticLaw>
              <listOfLocalParameters>
                <localParameter id="Kd_local"$value_attribute/>
              </listOfLocalParameters>
            </kineticLaw>
        """
    else
        """
            <kineticLaw>
              <math xmlns="http://www.w3.org/1998/Math/MathML"><ci>Kd_bind</ci></math>
            </kineticLaw>
        """
    end

    return """
    <?xml version="1.0" encoding="UTF-8"?>
    <sbml xmlns="http://www.sbml.org/sbml/level3/version2/core" level="3" version="2">
      <model id="kd_validation">
        $parameters
        <listOfSpecies>
          <species id="A" constant="false"/>
          <species id="B" constant="false"/>
          <species id="AB" constant="false"/>
        </listOfSpecies>
        <listOfReactions>
          <reaction id="bind" reversible="true">
            <listOfReactants>
              <speciesReference species="A" stoichiometry="1" constant="true"/>
              <speciesReference species="B" stoichiometry="1" constant="true"/>
            </listOfReactants>
            <listOfProducts>
              <speciesReference species="AB" stoichiometry="1" constant="true"/>
            </listOfProducts>
            $kinetic_law
          </reaction>
        </listOfReactions>
      </model>
    </sbml>
    """
end

function _post_sbml_import_for_validation(xml::AbstractString)
    return router(HTTP.Request(
        "POST",
        "/api/v1/import/sbml",
        ["Content-Type" => "application/json"],
        JSON3.write(Dict("sbml" => xml)),
    ))
end

@testset "SBML import enforces the canonical positive finite Kd invariant" begin
    for local_parameter in (false, true)
        source = local_parameter ? "local" : "global"
        for value in ("NaN", "Inf", "+Inf", "-Inf", "0", "-1")
            xml = _sbml_kd_validation_document(value;
                local_parameter=local_parameter)

            direct_error = try
                sbml_to_network_ir(xml)
                nothing
            catch err
                err
            end
            @test direct_error isa IRValidationError

            response = _post_sbml_import_for_validation(xml)
            @test response.status == 400
            @test occursin("reaction kd must be finite and > 0", String(response.body))
        end

        for (value, include_value) in (("garbage", true), ("", true), ("", false))
            xml = _sbml_kd_validation_document(value;
                local_parameter=local_parameter,
                include_value=include_value)

            direct_error = try
                sbml_to_network_ir(xml)
                nothing
            catch err
                err
            end
            @test direct_error isa IRValidationError
            @test occursin("Kd parameter", sprint(showerror, direct_error))

            response = _post_sbml_import_for_validation(xml)
            @test response.status == 400
            @test occursin("Kd parameter", String(response.body))
        end
    end

    # A Kd-like MathML reference is also explicit input.  If its global
    # declaration is absent, it must not collapse into the genuinely-missing
    # fallback below.
    unresolved_xml = _sbml_kd_validation_document("1"; local_parameter=false)
    unresolved_xml = replace(unresolved_xml,
        r"(?s)\s*<listOfParameters>.*?</listOfParameters>" => "")
    unresolved_error = try
        sbml_to_network_ir(unresolved_xml)
        nothing
    catch err
        err
    end
    @test unresolved_error isa IRValidationError
    @test occursin("has no declared global parameter",
        sprint(showerror, unresolved_error))
    unresolved_response = _post_sbml_import_for_validation(unresolved_xml)
    @test unresolved_response.status == 400
    @test occursin("has no declared global parameter",
        String(unresolved_response.body))

    # The conventional Kd_<reaction-id> fallback must inspect the declaration,
    # too; it cannot silently skip a bad value just because no MathML <ci>
    # references it.
    conventional_invalid_xml = _sbml_kd_validation_document("garbage";
        local_parameter=false)
    conventional_invalid_xml = replace(conventional_invalid_xml,
        r"(?s)\s*<kineticLaw>.*?</kineticLaw>" => "")
    conventional_error = try
        sbml_to_network_ir(conventional_invalid_xml)
        nothing
    catch err
        err
    end
    @test conventional_error isa IRValidationError
    @test occursin("global Kd parameter `Kd_bind`",
        sprint(showerror, conventional_error))
    conventional_response = _post_sbml_import_for_validation(
        conventional_invalid_xml)
    @test conventional_response.status == 400
    @test occursin("global Kd parameter `Kd_bind`",
        String(conventional_response.body))

    # An absent Kd is deliberately different from an explicit invalid value:
    # retain the established lossy-import fallback and make it visible.
    missing_xml = _sbml_kd_validation_document("1"; local_parameter=false)
    missing_xml = replace(missing_xml,
        r"(?s)\s*<listOfParameters>.*?</listOfParameters>" => "",
        r"(?s)\s*<kineticLaw>.*?</kineticLaw>" => "")
    network, warnings = sbml_to_network_ir(missing_xml)
    @test only(network.reactions).kd == 1.0
    @test any(warning -> occursin("defaulted Kd = 1.0", warning), warnings)

    response = _post_sbml_import_for_validation(missing_xml)
    @test response.status == 200
    body = JSON3.read(response.body)
    @test body["network_ir"]["reactions"][1]["kd"] == 1.0
    @test any(warning -> occursin("defaulted Kd = 1.0", String(warning)),
              body["warnings"])
end
