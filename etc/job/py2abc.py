import argparse
import ast
import os
from typing import List, Optional

# Lista de tipos que permitimos manter. O resto vira 'object'.
SAFE_TYPES = {
    "str",
    "int",
    "float",
    "bool",
    "bytes",
    "list",
    "dict",
    "tuple",
    "set",
    "None",
    "Any",
    "Optional",
    "Union",
    "IO",
    "Iterator",
    "List",
    "Dict",
    "Mapping",
    "Sequence",
    "Iterable",
    "datetime",
    "timedelta",
    "object",
}


def sanitize_type(node):
    """
    Recebe um nó de anotação de tipo (AST) e retorna uma versão sanitizada.
    Se o tipo não for seguro (built-in), substitui por 'object'.
    """
    if node is None:
        return None

    # Caso 1: Tipos Simples (str, int, CustomClass)
    if isinstance(node, ast.Name):
        if node.id in SAFE_TYPES:
            return node
        else:
            # Substitui tipos desconhecidos por 'object'
            return ast.Name(id="object", ctx=ast.Load())

    # Caso 2: Tipos Genéricos (List[str], Optional[Custom])
    elif isinstance(node, ast.Subscript):
        # Recursivamente sanitiza o conteúdo (o 'slice')
        # Ex: Optional[BadType] -> a slice é BadType, que vira object
        node.slice = sanitize_type(node.slice)
        return node

    # Caso 3: Constantes (ex: None)
    elif isinstance(node, ast.Constant):
        return node

    # Caso 4: Atributos (lib.Type) ou Binários (Type | None)
    # Simplificamos agressivamente para 'object' para evitar problemas de import
    return ast.Name(id="object", ctx=ast.Load())


def parse_methods_list(methods_str: str) -> List[str]:
    """Converte string de métodos para lista"""
    if not methods_str:
        return []

    # Remove colchetes e espaços, depois divide por vírgula
    methods_str = methods_str.strip("[]").replace(" ", "")
    if not methods_str:
        return []

    return [method.strip() for method in methods_str.split(",") if method.strip()]


def generate_abc_clean(
    file_path: str,
    class_name: str,
    methods: Optional[List[str]] = None,
    output_path: Optional[str] = None,
) -> str:
    """
    Gera uma interface ABC a partir de uma classe concreta.

    Args:
        file_path: Caminho para o arquivo Python
        class_name: Nome da classe a ser convertida em interface
        methods: Lista de métodos a serem incluídos (None para todos os públicos)
        output_path: Caminho de saída (None para retornar como string)

    Returns:
        String com o código da interface ou mensagem de sucesso
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        source_code = f.read()

    tree = ast.parse(source_code)

    target_class_node = None
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            target_class_node = node
            break

    if not target_class_node:
        raise ValueError(f"Classe '{class_name}' não encontrada")

    abc_methods = []
    fetch_all = not methods

    for node in target_class_node.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if fetch_all and node.name.startswith("_") and node.name != "__init__":
                continue
            if not fetch_all and node.name not in methods:
                continue

            # 1. Limpa o corpo
            node.body = [ast.Expr(value=ast.Constant(value=...))]

            # 2. Sanitiza Argumentos (Inputs)
            for arg in node.args.args:
                if arg.annotation:
                    arg.annotation = sanitize_type(arg.annotation)

            # Sanitiza argumentos keyword-only se houver
            for arg in node.args.kwonlyargs:
                if arg.annotation:
                    arg.annotation = sanitize_type(arg.annotation)

            # Sanitiza Defaults (substitui valores complexos por ...)
            if node.args.defaults:
                new_defaults = []
                for default in node.args.defaults:
                    if isinstance(default, (ast.Constant, ast.NameConstant)):
                        new_defaults.append(default)
                    else:
                        new_defaults.append(ast.Constant(value=None))
                node.args.defaults = new_defaults

            # 3. Sanitiza Retorno (Output)
            if node.returns:
                node.returns = sanitize_type(node.returns)

            # 4. Decorators
            kept_decorators = []
            for dec in node.decorator_list:
                try:
                    if ast.unparse(dec) in ["classmethod", "staticmethod", "property"]:
                        kept_decorators.append(dec)
                except:
                    pass

            kept_decorators.append(ast.Name(id="abstractmethod", ctx=ast.Load()))
            node.decorator_list = kept_decorators

            abc_methods.append(ast.unparse(node))

    # Montagem
    final_code = []
    final_code.append("from __future__ import annotations")
    final_code.append("from abc import ABC, abstractmethod")
    final_code.append(
        "from typing import Any, Optional, List, Dict, Union, IO, Iterator, Generator"
    )
    final_code.append("from datetime import datetime, timedelta")
    final_code.append("")
    final_code.append(f"class I{class_name}(ABC):")

    if not abc_methods:
        final_code.append("    pass")
    else:
        for method_str in abc_methods:
            indented_method = "\n".join(
                "    " + line for line in method_str.splitlines()
            )
            final_code.append(indented_method)
            final_code.append("")

    result = "\n".join(final_code)

    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result)
        return f"✅ Interface gerada em: {output_path}"
    else:
        return result


def setup_argparse() -> argparse.ArgumentParser:
    """Configura e retorna o parser de argumentos"""
    parser = argparse.ArgumentParser(
        description="Gera uma interface ABC (Abstract Base Class) a partir de uma classe concreta",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  python script.py --file_path ./my_class.py --class_name MyClass
  python script.py -f ./my_class.py -c MyClass -o ./output/IMyClass.py
  python script.py -f ./my_class.py -c MyClass -m "[method1, method2]" -o ./output/interface.py
        """,
    )

    parser.add_argument(
        "-f",
        "--file_path",
        type=str,
        required=True,
        help="Caminho para o arquivo Python contendo a classe",
    )

    parser.add_argument(
        "-c",
        "--class_name",
        type=str,
        required=True,
        help="Nome da classe a ser convertida em interface",
    )

    parser.add_argument(
        "-m",
        "--methods",
        type=str,
        default="",
        help='Lista de métodos específicos a incluir (ex: "[method1, method2]"). Se vazio, inclui todos os métodos públicos',
    )

    parser.add_argument(
        "-o",
        "--output_path",
        type=str,
        default=None,
        help="Caminho de saída para o arquivo de interface. Se não especificado, imprime na tela",
    )

    parser.add_argument(
        "--show_safe_types",
        action="store_true",
        help="Mostra a lista de tipos seguros e sai",
    )

    return parser


def main():
    """Função principal"""
    parser = setup_argparse()
    args = parser.parse_args()

    # Mostrar tipos seguros se solicitado
    if args.show_safe_types:
        print("Tipos seguros permitidos:")
        for safe_type in sorted(SAFE_TYPES):
            print(f"  - {safe_type}")
        return

    # Validar e normalizar caminhos (resolve ~, espaços, etc.)
    try:
        args.file_path = os.path.expanduser(args.file_path)
        if args.output_path:
            args.output_path = os.path.expanduser(args.output_path)
    except Exception as e:
        print(f"❌ Erro ao processar caminhos: {e}")
        return 1

    # Validar arquivo de entrada
    if not os.path.exists(args.file_path):
        print(f"❌ Erro: Arquivo não encontrado: {args.file_path}")
        return 1

    try:
        # Converter string de métodos para lista
        methods_list = parse_methods_list(args.methods)

        # Gerar interface
        result = generate_abc_clean(
            file_path=args.file_path,
            class_name=args.class_name,
            methods=methods_list if methods_list else None,
            output_path=args.output_path,
        )

        print(result)
        return 0

    except Exception as e:
        print(f"❌ Erro: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
