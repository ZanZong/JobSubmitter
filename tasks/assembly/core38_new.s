.section .data
    value1: .float 323.4343
    value2: .float 324.5344
.section .text
    .globl main
main:
      movq $84431, %rcx
      s38:
        cld
        divss  value1, %xmm0
        mulss  %xmm0, %xmm1
        mulss  %xmm0, %xmm1
        mulss  %xmm0, %xmm1
        mulss  %xmm0, %xmm1
        mulss  %xmm0, %xmm1
        mulss  %xmm0, %xmm1
        mulss  %xmm0, %xmm1
	loop s38
