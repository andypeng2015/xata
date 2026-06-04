function hasAtLeastOneXataScope() {
    return {
        Operation: {
            leave(operation, { report, location }) {
                const security = operation.security;
                const hasScope =
                    Array.isArray(security) &&
                    security.some(sec => Array.isArray(sec.xata) && sec.xata.length > 0);

                if (!hasScope) {
                    report({
                        message: 'Each operation must include at least one xata scope in its security declaration.',
                        location: location.child('security'),
                    });
                }
            },
        },
    };
}

const RESERVED_PATH_PARAMS = ['organizationID', 'projectID', 'branchID'];

function verifyReservedPathParameters() {
    return {
        PathItem(node, ctx) {
            for (const reserved of RESERVED_PATH_PARAMS) {
                for (const [method, operation] of Object.entries(node)) {
                    if (!['get', 'post', 'put', 'delete', 'patch'].includes(method.toLowerCase())) continue;
                    const params = (operation.parameters || node.parameters || []).map(param =>
                        param.$ref ? ctx.resolve(param).node : param
                    );
                    const found = params.some(p => p.in === 'path' && p.name === reserved);
                    if (ctx.key.includes(reserved) && !found) {
                        ctx.report({
                            message: `Path parameter '${reserved}' is reserved and must be defined in the path item parameters.`,
                        });
                    }
                }
            }
        }
    };
}

function addPublicServers() {
    return {
        Root: {
            leave(root) {
                root.servers = [
                    { url: "https://api.xata.tech", description: "Xata API" },
                ];
            },
        },
        PathItem: {
            leave(pathItem) {
                if (Array.isArray(pathItem.servers) && pathItem.servers.length === 0) {
                    delete pathItem.servers;
                }
            },
        },
    };
}

export default function () {
    return {
        id: "xata",
        rules: {
            oas3: {
                "has-at-least-one-xata-scope": hasAtLeastOneXataScope,
                "verify-reserved-path-parameters": verifyReservedPathParameters
            }
        },
        decorators: {
            oas3: {
                "add-public-servers": addPublicServers
            }
        }
    };
}
